//go:build kafkalive

// Live check of consumeLoop against a real Kafka cluster: restart the consumer group's coordinator broker while a
// sarama v1.43.3 consumer group (same defaults as this source) consumes. Not part of CI; needs KAFKA_LIVE_BROKERS
// and KAFKA_LIVE_RESTART (a command that restarts broker id $1 and returns once it is back).
package kafka

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type countingHandler struct{ n *int64 }

func (countingHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (countingHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h countingHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for m := range c.Messages() {
		atomic.AddInt64(h.n, 1)
		s.MarkMessage(m, "")
	}
	return nil
}

func runLive(t *testing.T, window time.Duration, restarts int) (panicked bool, panicMsg string, perRestart []int64) {
	brokers := strings.Split(os.Getenv("KAFKA_LIVE_BROKERS"), ",")
	topic, group := "nf-src", fmt.Sprintf("nf-live-%d", time.Now().UnixNano())
	cfg, err := configFromOpts("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cg, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		t.Fatal(err)
	}
	defer cg.Close()
	go func() {
		for e := range cg.Errors() {
			t.Logf("consumer group error (logged only): %v", e)
		}
	}()

	pcfg := sarama.NewConfig()
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.RequiredAcks = sarama.WaitForAll
	pcfg.Producer.Retry.Max = 1000
	prod, err := sarama.NewSyncProducer(brokers, pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { // ~40 msg/s for the whole run
		for i := 0; ctx.Err() == nil; i++ {
			_, _, _ = prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(fmt.Sprint(i))})
			time.Sleep(25 * time.Millisecond)
		}
	}()

	var consumed int64
	done := make(chan string, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Sprint(r)
				return
			}
			done <- ""
		}()
		consumeLoop(ctx, func() error { return cg.Consume(ctx, []string{topic}, countingHandler{&consumed}) }, zap.NewExample().Sugar(), window)
	}()

	waitConsuming := func(d time.Duration) int64 { // returns messages consumed in a d-long window
		a := atomic.LoadInt64(&consumed)
		time.Sleep(d)
		return atomic.LoadInt64(&consumed) - a
	}
	if n := waitConsuming(10 * time.Second); n == 0 {
		t.Fatal("consumer never started")
	}
	for r := 0; r < restarts; r++ {
		coord, err := client.Coordinator(group)
		if err != nil {
			t.Fatalf("coordinator lookup: %v", err)
		}
		t.Logf("restart %d: coordinator is broker %d", r+1, coord.ID())
		out, err := exec.Command("bash", "-c", os.Getenv("KAFKA_LIVE_RESTART")+" "+fmt.Sprint(coord.ID())).CombinedOutput()
		if err != nil {
			t.Fatalf("restart: %v %s", err, out)
		}
		select {
		case msg := <-done:
			return true, msg, perRestart
		default:
		}
		n := waitConsuming(30 * time.Second)
		select {
		case msg := <-done:
			return true, msg, append(perRestart, n)
		default:
		}
		t.Logf("restart %d: %d messages consumed in the 30 s after the broker came back", r+1, n)
		perRestart = append(perRestart, n)
	}
	cancel()
	return false, "", perRestart
}

func TestLive_LegacyBehaviourPanicsOnCoordinatorRestart(t *testing.T) {
	// window 0 = the v1.4.6 behaviour: the first Consume error panics
	panicked, msg, _ := runLive(t, 0, 3)
	t.Logf("legacy: panicked=%v %s", panicked, msg)
	if !panicked {
		t.Fatal("expected the old behaviour to panic on a coordinator restart (reproduction failed)")
	}
}

func TestLive_PatchedSurvivesCoordinatorRestarts(t *testing.T) {
	panicked, msg, per := runLive(t, consumeRetryWindow, 3)
	if panicked {
		t.Fatalf("patched consumer panicked: %s", msg)
	}
	for i, n := range per {
		if n == 0 {
			t.Fatalf("no messages consumed after restart %d", i+1)
		}
	}
	t.Logf("patched: survived %d coordinator restarts, consumed per 30 s after each: %v", len(per), per)
}
