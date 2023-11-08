package mock

import (
	"sync"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/registryutil"
)

type MetricsVector interface {
	With(map[string]string) interface{}

	// Reset deletes all metrics in vector.
	Reset()
}

// Vector is base implementation of vector of metrics of any supported type
type Vector struct {
	Labels    []string
	Mtx       sync.RWMutex // Protects metrics.
	Metrics   map[uint64]interface{}
	NewMetric func(map[string]string) interface{}
}

func (v *Vector) With(tags map[string]string) interface{} {
	hv, err := registryutil.VectorHash(tags, v.Labels)
	if err != nil {
		panic(err)
	}

	v.Mtx.RLock()
	metric, ok := v.Metrics[hv]
	v.Mtx.RUnlock()
	if ok {
		return metric
	}

	v.Mtx.Lock()
	defer v.Mtx.Unlock()

	metric, ok = v.Metrics[hv]
	if !ok {
		metric = v.NewMetric(tags)
		v.Metrics[hv] = metric
	}

	return metric
}

// Reset deletes all metrics in this vector.
func (v *Vector) Reset() {
	v.Mtx.Lock()
	defer v.Mtx.Unlock()

	for h := range v.Metrics {
		delete(v.Metrics, h)
	}
}

var _ metrics.CounterVec = (*CounterVec)(nil)

// CounterVec stores counters and
// implements metrics.CounterVec interface
type CounterVec struct {
	Vec MetricsVector
}

// CounterVec creates a new counters vector with given metric name and
// partitioned by the given label names.
func (r *Registry) CounterVec(name string, labels []string) metrics.CounterVec {
	return &CounterVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).Counter(name)
			},
		},
	}
}

// With creates new or returns existing counter with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *CounterVec) With(tags map[string]string) metrics.Counter {
	return v.Vec.With(tags).(*Counter)
}

// Reset deletes all metrics in this vector.
func (v *CounterVec) Reset() {
	v.Vec.Reset()
}

var _ metrics.GaugeVec = new(GaugeVec)

// GaugeVec stores gauges and
// implements metrics.GaugeVec interface
type GaugeVec struct {
	Vec MetricsVector
}

// GaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) GaugeVec(name string, labels []string) metrics.GaugeVec {
	return &GaugeVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).Gauge(name)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *GaugeVec) With(tags map[string]string) metrics.Gauge {
	return v.Vec.With(tags).(*Gauge)
}

// Reset deletes all metrics in this vector.
func (v *GaugeVec) Reset() {
	v.Vec.Reset()
}

var _ metrics.IntGaugeVec = new(IntGaugeVec)

// IntGaugeVec stores gauges and
// implements metrics.IntGaugeVec interface
type IntGaugeVec struct {
	Vec MetricsVector
}

// IntGaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) IntGaugeVec(name string, labels []string) metrics.IntGaugeVec {
	return &IntGaugeVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).IntGauge(name)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *IntGaugeVec) With(tags map[string]string) metrics.IntGauge {
	return v.Vec.With(tags).(*IntGauge)
}

// Reset deletes all metrics in this vector.
func (v *IntGaugeVec) Reset() {
	v.Vec.Reset()
}

var _ metrics.TimerVec = new(TimerVec)

// TimerVec stores timers and
// implements metrics.TimerVec interface
type TimerVec struct {
	Vec MetricsVector
}

// TimerVec creates a new timers vector with given metric name and
// partitioned by the given label names.
func (r *Registry) TimerVec(name string, labels []string) metrics.TimerVec {
	return &TimerVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).Timer(name)
			},
		},
	}
}

// With creates new or returns existing timer with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *TimerVec) With(tags map[string]string) metrics.Timer {
	return v.Vec.With(tags).(*Timer)
}

// Reset deletes all metrics in this vector.
func (v *TimerVec) Reset() {
	v.Vec.Reset()
}

var _ metrics.HistogramVec = (*HistogramVec)(nil)

// HistogramVec stores histograms and
// implements metrics.HistogramVec interface
type HistogramVec struct {
	Vec MetricsVector
}

// HistogramVec creates a new histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	return &HistogramVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).Histogram(name, buckets)
			},
		},
	}
}

// With creates new or returns existing histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *HistogramVec) With(tags map[string]string) metrics.Histogram {
	return v.Vec.With(tags).(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *HistogramVec) Reset() {
	v.Vec.Reset()
}

var _ metrics.TimerVec = (*DurationHistogramVec)(nil)

// DurationHistogramVec stores duration histograms and
// implements metrics.TimerVec interface
type DurationHistogramVec struct {
	Vec MetricsVector
}

// DurationHistogramVec creates a new duration histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
	return &DurationHistogramVec{
		Vec: &Vector{
			Labels:  append([]string(nil), labels...),
			Metrics: make(map[uint64]interface{}),
			NewMetric: func(tags map[string]string) interface{} {
				return r.WithTags(tags).DurationHistogram(name, buckets)
			},
		},
	}
}

// With creates new or returns existing duration histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *DurationHistogramVec) With(tags map[string]string) metrics.Timer {
	return v.Vec.With(tags).(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *DurationHistogramVec) Reset() {
	v.Vec.Reset()
}
