//go:build go1.18

package errors

import (
	"testing"
)

func TestMust(t *testing.T) {
	if got := Must(10, nil); got != 10 {
		t.Fatalf("Expected %+v, got %+v", 10, got)
	}

	panics := func() (r bool) {
		defer func() {
			if recover() != nil {
				r = true
			}
		}()
		Must(10, New("test error"))
		return r
	}()
	if !panics {
		t.Fatal("Panic expected")
	}
}
