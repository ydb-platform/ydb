package mock

import (
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var _ metrics.Counter = (*Counter)(nil)

// Counter tracks monotonically increasing value.
type Counter struct {
	Name  string
	Tags  map[string]string
	Value *atomic.Int64
}

// Inc increments counter by 1.
func (c *Counter) Inc() {
	c.Add(1)
}

// Add adds delta to the counter. Delta must be >=0.
func (c *Counter) Add(delta int64) {
	c.Value.Add(delta)
}

var _ metrics.FuncCounter = (*FuncCounter)(nil)

type FuncCounter struct {
	function func() int64
}

func (c FuncCounter) Function() func() int64 {
	return c.function
}
