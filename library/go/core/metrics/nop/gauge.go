package nop

import "github.com/ydb-platform/ydb/library/go/core/metrics"

var _ metrics.Gauge = (*Gauge)(nil)

type Gauge struct{}

func (Gauge) Set(_ float64) {}

func (Gauge) Add(_ float64) {}

var _ metrics.GaugeVec = (*GaugeVec)(nil)

type GaugeVec struct{}

func (t GaugeVec) With(_ map[string]string) metrics.Gauge {
	return Gauge{}
}

func (t GaugeVec) Reset() {}

var _ metrics.FuncGauge = (*FuncGauge)(nil)

type FuncGauge struct {
	function func() float64
}

func (g FuncGauge) Function() func() float64 {
	return g.function
}
