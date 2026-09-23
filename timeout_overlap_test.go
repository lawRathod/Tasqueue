package tasqueue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestTimeoutAttemptOverlap is the minimal reproducer for overlapping attempts.
//
// A job's per-attempt Timeout abandons the attempt but does not stop it: the
// handler runs on its own goroutine and execJob returns without joining it, so
// retryJob re-enqueues while the previous attempt is still running. The handler
// here does not poll ctx (time.Sleep cannot), which the handler contract permits.
//
// The test asserts the property a retry budget is normally taken to imply: a job
// is executing at most once at a time, however many attempts it is allowed. It
// fails on the current code.
func TestTimeoutAttemptOverlap(t *testing.T) {
	ctx := context.Background()

	var (
		mu         sync.Mutex
		inflight   = map[string]int{}
		maxOverlap int
	)
	srv := newServer(t, "slow", func(_ []byte, jctx JobCtx) error {
		mu.Lock()
		inflight[jctx.Meta.ID]++
		if inflight[jctx.Meta.ID] > maxOverlap {
			maxOverlap = inflight[jctx.Meta.ID]
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inflight[jctx.Meta.ID]--
			mu.Unlock()
		}()
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	go srv.Start(ctx)

	// 20 ms deadline against 200 ms of service, three attempts.
	job, err := NewJob("slow", nil, JobOpts{MaxRetries: 2, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	// Let every attempt start and the last one time out.
	time.Sleep(time.Second)

	mu.Lock()
	max := maxOverlap
	mu.Unlock()

	if max > 1 {
		t.Errorf("max concurrent handler executions of one job = %d, want <= 1: a retry overlapped the attempt it abandoned", max)
	}
}
