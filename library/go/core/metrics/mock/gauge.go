package mock

import (
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var _ metrics.Gauge = (*Gauge)(nil)

// Gauge tracks single float64 value.
type Gauge struct {
	Name  string
	Tags  map[string]string
	Value *atomic.Float64
}

func (g *Gauge) Set(value float64) {
	g.Value.Store(value)
}

func (g *Gauge) Add(value float64) {
	g.Value.Add(value)
}

var _ metrics.FuncGauge = (*FuncGauge)(nil)

type FuncGauge struct {
	function func() float64
}

func (g FuncGauge) Function() func() float64 {
	return g.function
}
