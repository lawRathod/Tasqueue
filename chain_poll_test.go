package tasqueue

import (
	"context"
	"testing"
	"time"
)

// TestGetChainPollDoesNotDuplicatePrevJobs is the minimal reproducer for the
// chain read path corrupting the record it returns.
//
// ChainMessage.PrevJobs is "list of IDs of completed jobs" and ChainMeta.JobID is
// "ID of the current job part of chain". GetChain walks from c.JobID each call,
// appends every DONE job it passes to PrevJobs, and never moves c.JobID forward.
// So while a chain is in flight, every poll re-appends the same completed job:
//
//	poll 1: PrevJobs=[step1]
//	poll 2: PrevJobs=[step1, step1]
//	poll 3: PrevJobs=[step1, step1, step1]
//
// and JobID stays on step 1 even after step 2 is running. The test asserts the
// documented behaviour: distinct completed jobs, and a cursor that advances.
func TestGetChainPollDoesNotDuplicatePrevJobs(t *testing.T) {
	ctx := context.Background()
	// Step one is immediate; step two is slow, so the chain is reliably observed
	// with step 1 complete and step 2 running.
	srv := newServer(t, "step", func(p []byte, jctx JobCtx) error {
		if err := jctx.Save([]byte("r")); err != nil {
			return err
		}
		if string(p) == "two" {
			time.Sleep(500 * time.Millisecond)
		}
		return nil
	})
	go srv.Start(ctx)

	j1, _ := NewJob("step", []byte("one"), JobOpts{MaxRetries: 0})
	j2, _ := NewJob("step", []byte("two"), JobOpts{MaxRetries: 0})
	chain, _ := NewChain([]Job{j1, j2}, ChainOpts{ID: "c-poll"})
	if _, err := srv.EnqueueChain(ctx, chain); err != nil {
		t.Fatal(err)
	}

	// Let step 1 finish and step 2 start.
	time.Sleep(150 * time.Millisecond)

	var (
		lengths []int
		last    ChainMessage
	)
	for range 3 {
		c, err := srv.GetChain(ctx, "c-poll")
		if err != nil {
			t.Fatalf("GetChain: %v", err)
		}
		lengths = append(lengths, len(c.PrevJobs))
		last = c
	}

	// A read must not grow the record while the chain itself has not advanced.
	for i := 1; i < len(lengths); i++ {
		if lengths[i] != lengths[0] {
			t.Errorf("PrevJobs grew across polls of an unchanged chain: lengths=%v", lengths)
			break
		}
	}
	// PrevJobs names completed jobs: the same job must not appear twice.
	seen := map[string]int{}
	for _, id := range last.PrevJobs {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("job %s named %d times in PrevJobs %v", id, n, last.PrevJobs)
		}
	}
	// JobID is the chain's CURRENT job, so it must not also be listed as a
	// completed one.
	if seen[last.JobID] > 0 {
		t.Errorf("JobID %s is the current job but also appears in PrevJobs %v", last.JobID, last.PrevJobs)
	}
}
