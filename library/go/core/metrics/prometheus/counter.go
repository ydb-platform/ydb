package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

var _ metrics.Counter = (*Counter)(nil)

// Counter tracks monotonically increasing value.
type Counter struct {
	cnt prometheus.Counter
}

// Inc increments counter by 1.
func (c Counter) Inc() {
	c.cnt.Inc()
}

// Add adds delta to the counter. Delta must be >=0.
func (c Counter) Add(delta int64) {
	c.cnt.Add(float64(delta))
}

var _ metrics.FuncCounter = (*FuncCounter)(nil)

type FuncCounter struct {
	cnt      prometheus.CounterFunc
	function func() int64
}

func (c FuncCounter) Function() func() int64 {
	return c.function
}
