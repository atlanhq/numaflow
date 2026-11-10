package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger { return zap.NewNop().Sugar() }

// errs returns a consume func that returns the given errors in order, then cancels ctx.
func errs(cancel context.CancelFunc, calls *int32, seq ...error) func() error {
	return func() error {
		i := atomic.AddInt32(calls, 1) - 1
		if int(i) < len(seq) {
			return seq[i]
		}
		cancel()
		return nil
	}
}

func TestConsumeLoop_RetriesCoordinatorMoveThenContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int32
	// what a broker restart produced on the dev tenants: JoinGroup to a shutting-down coordinator
	consume := errs(cancel, &calls, sarama.ErrConsumerCoordinatorNotAvailable, sarama.ErrOutOfBrokers, nil)
	done := make(chan struct{})
	go func() { consumeLoop(ctx, consume, testLogger(), time.Minute); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("consumeLoop did not return")
	}
	assert.EqualValues(t, 4, atomic.LoadInt32(&calls)) // 2 retried errors, 1 successful session, then ctx cancelled
}

func TestConsumeLoop_NonRetriableErrorPanics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int32
	consume := errs(cancel, &calls, sarama.ErrTopicAuthorizationFailed)
	assert.Panics(t, func() { consumeLoop(ctx, consume, testLogger(), time.Minute) })
	assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
}

func TestConsumeLoop_RetriableErrorOutlastingWindowPanics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consume := func() error { return sarama.ErrConsumerCoordinatorNotAvailable }
	start := time.Now()
	assert.Panics(t, func() { consumeLoop(ctx, consume, testLogger(), 1500*time.Millisecond) })
	assert.GreaterOrEqual(t, time.Since(start), 1500*time.Millisecond)
}

func TestConsumeLoop_ClosedConsumerGroupReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int32
	consume := errs(cancel, &calls, fmt.Errorf("wrapped: %w", sarama.ErrClosedConsumerGroup))
	assert.NotPanics(t, func() { consumeLoop(ctx, consume, testLogger(), time.Minute) })
	assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
}

func TestConsumeLoop_ContextCancelledDuringBackoffReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consume := func() error {
		go func() { time.Sleep(100 * time.Millisecond); cancel() }()
		return sarama.ErrConsumerCoordinatorNotAvailable
	}
	done := make(chan struct{})
	go func() { consumeLoop(ctx, consume, testLogger(), time.Minute); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeLoop did not stop on context cancel during backoff")
	}
}

func TestConsumeLoop_SuccessResetsTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int32
	// two failure bursts separated by a successful session; each burst is shorter than the window, the total is not
	e := sarama.ErrConsumerCoordinatorNotAvailable
	consume := errs(cancel, &calls, e, e, nil, e, e, nil)
	assert.NotPanics(t, func() { consumeLoop(ctx, consume, testLogger(), 2500*time.Millisecond) })
	assert.EqualValues(t, 7, atomic.LoadInt32(&calls))
}

func TestIsRetriableConsumeError(t *testing.T) {
	for _, e := range []error{
		sarama.ErrConsumerCoordinatorNotAvailable, sarama.ErrNotCoordinatorForConsumer, sarama.ErrOffsetsLoadInProgress,
		sarama.ErrRebalanceInProgress, sarama.ErrLeaderNotAvailable, sarama.ErrNotLeaderForPartition,
		sarama.ErrRequestTimedOut, sarama.ErrOutOfBrokers, sarama.ErrNotConnected,
		fmt.Errorf("refresh metadata: %w", sarama.ErrOutOfBrokers),
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, io.EOF,
		fmt.Errorf("join group: %w", syscall.ECONNRESET),
	} {
		assert.True(t, isRetriableConsumeError(e), "%v should be retriable", e)
	}
	for _, e := range []error{
		sarama.ErrTopicAuthorizationFailed, sarama.ErrGroupAuthorizationFailed, sarama.ErrUnknownTopicOrPartition,
		sarama.ErrInvalidGroupId, sarama.ErrClosedConsumerGroup, errors.New("something else"),
	} {
		assert.False(t, isRetriableConsumeError(e), "%v should not be retriable", e)
	}
}

// seqFn returns a func that yields the given errors in order, then succeeds with the value ok.
func seqFn(calls *int32, seq ...error) func() (string, error) {
	return func() (string, error) {
		i := atomic.AddInt32(calls, 1) - 1
		if int(i) < len(seq) {
			return "", seq[i]
		}
		return "ok", nil
	}
}

func TestRetryTransient_BrokerDownAtStartupThenUp(t *testing.T) {
	var calls int32
	// what a source starting while its single broker restarts gets from sarama.NewClient
	outOfBrokers := fmt.Errorf("%w: dial tcp 10.0.0.1:9092: connect: %w", sarama.ErrOutOfBrokers, syscall.ECONNREFUSED)
	v, err := retryTransient(context.Background(), "create sarama client", seqFn(&calls, outOfBrokers, outOfBrokers), testLogger(), time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, "ok", v)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

func TestRetryTransient_NonRetriableReturnsAtOnce(t *testing.T) {
	var calls int32
	bad := sarama.ConfigurationError("bad config")
	_, err := retryTransient(context.Background(), "create sarama client", seqFn(&calls, bad, nil), testLogger(), time.Minute)
	assert.ErrorIs(t, err, bad)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestRetryTransient_GivesUpAfterTheWindow(t *testing.T) {
	var calls int32
	_, err := retryTransient(context.Background(), "create sarama client", func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", sarama.ErrOutOfBrokers
	}, testLogger(), 1500*time.Millisecond)
	assert.ErrorIs(t, err, sarama.ErrOutOfBrokers)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}

func TestRetryTransient_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int32
	_, err := retryTransient(ctx, "create sarama client", seqFn(&calls, io.EOF, io.EOF, io.EOF), testLogger(), time.Minute)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}
