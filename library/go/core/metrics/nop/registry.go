package nop

import "github.com/ydb-platform/ydb/library/go/core/metrics"

var _ metrics.Registry = (*Registry)(nil)

type Registry struct{}

func (r Registry) ComposeName(parts ...string) string {
	return ""
}

func (r Registry) WithTags(_ map[string]string) metrics.Registry {
	return Registry{}
}

func (r Registry) WithPrefix(_ string) metrics.Registry {
	return Registry{}
}

func (r Registry) Counter(_ string) metrics.Counter {
	return Counter{}
}

func (r Registry) FuncCounter(_ string, function func() int64) metrics.FuncCounter {
	return FuncCounter{function: function}
}

func (r Registry) Gauge(_ string) metrics.Gauge {
	return Gauge{}
}

func (r Registry) FuncGauge(_ string, function func() float64) metrics.FuncGauge {
	return FuncGauge{function: function}
}

func (r Registry) IntGauge(_ string) metrics.IntGauge {
	return IntGauge{}
}

func (r Registry) FuncIntGauge(_ string, function func() int64) metrics.FuncIntGauge {
	return FuncIntGauge{function: function}
}

func (r Registry) Timer(_ string) metrics.Timer {
	return Timer{}
}

func (r Registry) Histogram(_ string, _ metrics.Buckets) metrics.Histogram {
	return Histogram{}
}

func (r Registry) DurationHistogram(_ string, _ metrics.DurationBuckets) metrics.Timer {
	return Histogram{}
}

func (r Registry) CounterVec(_ string, _ []string) metrics.CounterVec {
	return CounterVec{}
}

func (r Registry) GaugeVec(_ string, _ []string) metrics.GaugeVec {
	return GaugeVec{}
}

func (r Registry) IntGaugeVec(_ string, _ []string) metrics.IntGaugeVec {
	return IntGaugeVec{}
}

func (r Registry) TimerVec(_ string, _ []string) metrics.TimerVec {
	return TimerVec{}
}

func (r Registry) HistogramVec(_ string, _ metrics.Buckets, _ []string) metrics.HistogramVec {
	return HistogramVec{}
}

func (r Registry) DurationHistogramVec(_ string, _ metrics.DurationBuckets, _ []string) metrics.TimerVec {
	return DurationHistogramVec{}
}
