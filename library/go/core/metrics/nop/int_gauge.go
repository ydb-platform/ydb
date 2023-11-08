package nop

import "github.com/ydb-platform/ydb/library/go/core/metrics"

var _ metrics.IntGauge = (*IntGauge)(nil)

type IntGauge struct{}

func (IntGauge) Set(_ int64) {}

func (IntGauge) Add(_ int64) {}

var _ metrics.IntGaugeVec = (*IntGaugeVec)(nil)

type IntGaugeVec struct{}

func (t IntGaugeVec) With(_ map[string]string) metrics.IntGauge {
	return IntGauge{}
}

func (t IntGaugeVec) Reset() {}

var _ metrics.FuncIntGauge = (*FuncIntGauge)(nil)

type FuncIntGauge struct {
	function func() int64
}

func (g FuncIntGauge) Function() func() int64 {
	return g.function
}
