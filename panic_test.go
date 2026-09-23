package tasqueue

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestHandlerPanicTakesDownTheProcess is the minimal reproducer for the worker
// having no panic isolation.
//
// execJob runs the handler on its own goroutine with no recover
// (server.go:399-401). An unrecovered panic in any goroutine terminates the
// process, so a single handler that panics — a malformed payload, a nil map, a
// library that panics — takes the whole queue server with it, including every
// other in-flight job.
//
// A crash cannot be asserted in-process: the test binary would die with it. The
// crashing server therefore runs in a subprocess and the parent asserts the
// process outcome, which is exactly the behaviour under test.
func TestHandlerPanicTakesDownTheProcess(t *testing.T) {
	if os.Getenv("TASQ_PANIC_CHILD") == "1" {
		ctx := context.Background()
		srv := newServer(t, "boom", func(_ []byte, _ JobCtx) error {
			panic("handler exploded")
		})
		go srv.Start(ctx)
		job, err := NewJob("boom", nil, JobOpts{MaxRetries: 0})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := srv.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		fmt.Println("CHILD SURVIVED")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestHandlerPanicTakesDownTheProcess", "-test.v")
	cmd.Env = append(os.Environ(), "TASQ_PANIC_CHILD=1")
	out, err := cmd.CombinedOutput()

	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) > 8 {
		lines = lines[len(lines)-8:]
	}
	if err == nil {
		t.Fatalf("the server survived a panicking handler (worker recovered):\n%s", strings.Join(lines, "\n"))
	}
	t.Errorf("a panicking handler terminated the whole server process (%v); stack tail:\n%s",
		err, strings.Join(lines, "\n"))
}
