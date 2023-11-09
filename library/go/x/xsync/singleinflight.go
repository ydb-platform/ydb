package xsync

import (
	"sync"
	"sync/atomic"
)

// SingleInflight allows only one execution of function at time.
// For more exhaustive functionality see https://pkg.go.dev/golang.org/x/sync/singleflight.
type SingleInflight struct {
	updatingOnce atomic.Value
}

// NewSingleInflight creates new SingleInflight.
func NewSingleInflight() SingleInflight {
	var v atomic.Value
	v.Store(new(sync.Once))
	return SingleInflight{updatingOnce: v}
}

// Do executes the given function, making sure that only one execution is in-flight.
// If another caller comes in, it waits for the original to complete.
func (i *SingleInflight) Do(f func()) {
	i.getOnce().Do(func() {
		f()
		i.setOnce()
	})
}

func (i *SingleInflight) getOnce() *sync.Once {
	return i.updatingOnce.Load().(*sync.Once)
}

func (i *SingleInflight) setOnce() {
	i.updatingOnce.Store(new(sync.Once))
}
