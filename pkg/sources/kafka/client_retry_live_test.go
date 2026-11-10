//go:build kafkalive

package kafka

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Live: a broker that is down when the source creates its client, then comes back.
// Needs a stopped container named by KAFKA_LIVE_CONTAINER publishing localhost:9092 (single-node KRaft).
func liveContainer(t *testing.T) string {
	c := os.Getenv("KAFKA_LIVE_CONTAINER")
	if c == "" {
		t.Skip("KAFKA_LIVE_CONTAINER not set")
	}
	return c
}

func liveConfig() *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V2_0_0_0
	c.Net.DialTimeout = 2 * time.Second
	c.Metadata.Retry.Max = 1
	return c
}

func TestLive_ClientCreationWaitsForBrokerThenSucceeds(t *testing.T) {
	c := liveContainer(t)
	logger := zap.NewExample().Sugar()
	go func() { time.Sleep(20 * time.Second); _ = exec.Command("docker", "start", c).Run() }()
	start := time.Now()
	client, err := retryTransient(context.Background(), "create sarama client", func() (sarama.Client, error) {
		return sarama.NewClient([]string{"localhost:9092"}, liveConfig())
	}, logger, consumeRetryWindow)
	assert.NoError(t, err)
	admin, err := retryTransient(context.Background(), "create sarama cluster admin client", func() (sarama.ClusterAdmin, error) {
		return sarama.NewClusterAdminFromClient(client)
	}, logger, consumeRetryWindow)
	assert.NoError(t, err)
	topics, err := admin.ListTopics()
	assert.NoError(t, err)
	t.Logf("client+admin ready after %s (broker started at ~20s); %d topics", time.Since(start).Round(time.Second), len(topics))
	assert.Greater(t, time.Since(start), 15*time.Second)
	_ = admin.Close()
}

func TestLive_BrokerDownPastTheWindowStillFails(t *testing.T) {
	c := liveContainer(t)
	_ = exec.Command("docker", "stop", c).Run()
	start := time.Now()
	_, err := retryTransient(context.Background(), "create sarama client", func() (sarama.Client, error) {
		return sarama.NewClient([]string{"localhost:9092"}, liveConfig())
	}, zap.NewExample().Sugar(), 15*time.Second)
	assert.ErrorIs(t, err, sarama.ErrOutOfBrokers)
	t.Logf("gave up after %s with: %v", time.Since(start).Round(time.Second), err)
	assert.Less(t, time.Since(start), 60*time.Second)
}
