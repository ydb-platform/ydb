package mock

import (
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var _ metrics.IntGauge = (*IntGauge)(nil)

// IntGauge tracks single int64 value.
type IntGauge struct {
	Name  string
	Tags  map[string]string
	Value *atomic.Int64
}

func (g *IntGauge) Set(value int64) {
	g.Value.Store(value)
}

func (g *IntGauge) Add(value int64) {
	g.Value.Add(value)
}

var _ metrics.FuncIntGauge = (*FuncIntGauge)(nil)

type FuncIntGauge struct {
	function func() int64
}

func (g FuncIntGauge) Function() func() int64 {
	return g.function
}
