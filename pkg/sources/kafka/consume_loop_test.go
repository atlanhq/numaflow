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
