// Package metrics provides interface collecting performance metrics.
package metrics

import (
	"context"
	"io"
	"time"
)

// Gauge tracks single float64 value.
type Gauge interface {
	Set(value float64)
	Add(value float64)
}

// FuncGauge is Gauge with value provided by callback function.
type FuncGauge interface {
	Function() func() float64
}

// IntGauge tracks single int64 value.
type IntGauge interface {
	Set(value int64)
	Add(value int64)
}

// FuncIntGauge is IntGauge with value provided by callback function.
type FuncIntGauge interface {
	Function() func() int64
}

// Counter tracks monotonically increasing value.
type Counter interface {
	// Inc increments counter by 1.
	Inc()

	// Add adds delta to the counter. Delta must be >=0.
	Add(delta int64)
}

// FuncCounter is Counter with value provided by callback function.
type FuncCounter interface {
	Function() func() int64
}

// Histogram tracks distribution of value.
type Histogram interface {
	RecordValue(value float64)
}

// Timer measures durations.
type Timer interface {
	RecordDuration(value time.Duration)
}

// DurationBuckets defines buckets of the duration histogram.
type DurationBuckets interface {
	// Size returns number of buckets.
	Size() int

	// MapDuration returns index of the bucket.
	//
	// index is integer in range [0, Size()).
	MapDuration(d time.Duration) int

	// UpperBound of the last bucket is always +Inf.
	//
	// bucketIndex is integer in range [0, Size()-1).
	UpperBound(bucketIndex int) time.Duration
}

// Buckets defines intervals of the regular histogram.
type Buckets interface {
	// Size returns number of buckets.
	Size() int

	// MapValue returns index of the bucket.
	//
	// Index is integer in range [0, Size()).
	MapValue(v float64) int

	// UpperBound of the last bucket is always +Inf.
	//
	// bucketIndex is integer in range [0, Size()-1).
	UpperBound(bucketIndex int) float64
}

// GaugeVec stores multiple dynamically created gauges.
type GaugeVec interface {
	With(map[string]string) Gauge

	// Reset deletes all metrics in vector.
	Reset()
}

// IntGaugeVec stores multiple dynamically created gauges.
type IntGaugeVec interface {
	With(map[string]string) IntGauge

	// Reset deletes all metrics in vector.
	Reset()
}

// CounterVec stores multiple dynamically created counters.
type CounterVec interface {
	With(map[string]string) Counter

	// Reset deletes all metrics in vector.
	Reset()
}

// TimerVec stores multiple dynamically created timers.
type TimerVec interface {
	With(map[string]string) Timer

	// Reset deletes all metrics in vector.
	Reset()
}

// HistogramVec stores multiple dynamically created histograms.
type HistogramVec interface {
	With(map[string]string) Histogram

	// Reset deletes all metrics in vector.
	Reset()
}

// Registry creates profiling metrics.
type Registry interface {
	// WithTags creates new sub-scope, where each metric has tags attached to it.
	WithTags(tags map[string]string) Registry
	// WithPrefix creates new sub-scope, where each metric has prefix added to it name.
	WithPrefix(prefix string) Registry

	ComposeName(parts ...string) string

	Counter(name string) Counter
	CounterVec(name string, labels []string) CounterVec
	FuncCounter(name string, function func() int64) FuncCounter

	Gauge(name string) Gauge
	GaugeVec(name string, labels []string) GaugeVec
	FuncGauge(name string, function func() float64) FuncGauge

	IntGauge(name string) IntGauge
	IntGaugeVec(name string, labels []string) IntGaugeVec
	FuncIntGauge(name string, function func() int64) FuncIntGauge

	Timer(name string) Timer
	TimerVec(name string, labels []string) TimerVec

	Histogram(name string, buckets Buckets) Histogram
	HistogramVec(name string, buckets Buckets, labels []string) HistogramVec

	DurationHistogram(name string, buckets DurationBuckets) Timer
	DurationHistogramVec(name string, buckets DurationBuckets, labels []string) TimerVec
}

// MetricsStreamer represents a registry that can stream collected metrics data to some destination
type MetricsStreamer interface {
	Stream(context.Context, io.Writer) (int, error)
}

// CollectPolicy defines how registered gauge metrics are updated via collect func.
type CollectPolicy interface {
	RegisteredCounter(counterFunc func() int64) func() int64
	RegisteredGauge(gaugeFunc func() float64) func() float64
	AddCollect(collect func(ctx context.Context))
}
