package solomon

import (
	"sync"

	"a.yandex-team.ru/library/go/core/metrics"
	"a.yandex-team.ru/library/go/core/metrics/internal/pkg/registryutil"
)

// metricsVector is a base implementation of vector of metrics of any supported type.
type metricsVector struct {
	labels    []string
	mtx       sync.RWMutex // Protects metrics.
	metrics   map[uint64]Metric
	rated     bool
	newMetric func(map[string]string) Metric
}

func (v *metricsVector) with(tags map[string]string) Metric {
	hv, err := registryutil.VectorHash(tags, v.labels)
	if err != nil {
		panic(err)
	}

	v.mtx.RLock()
	metric, ok := v.metrics[hv]
	v.mtx.RUnlock()
	if ok {
		return metric
	}

	v.mtx.Lock()
	defer v.mtx.Unlock()

	metric, ok = v.metrics[hv]
	if !ok {
		metric = v.newMetric(tags)
		v.metrics[hv] = metric
	}

	return metric
}

// reset deletes all metrics in this vector.
func (v *metricsVector) reset() {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	for h := range v.metrics {
		delete(v.metrics, h)
	}
}

var _ metrics.CounterVec = (*CounterVec)(nil)

// CounterVec stores counters and
// implements metrics.CounterVec interface.
type CounterVec struct {
	vec *metricsVector
}

// CounterVec creates a new counters vector with given metric name and
// partitioned by the given label names.
func (r *Registry) CounterVec(name string, labels []string) metrics.CounterVec {
	var vec *metricsVector
	vec = &metricsVector{
		labels:  append([]string(nil), labels...),
		metrics: make(map[uint64]Metric),
		rated:   r.rated,
		newMetric: func(tags map[string]string) Metric {
			return r.Rated(vec.rated).
				WithTags(tags).
				Counter(name).(*Counter)
		},
	}
	return &CounterVec{vec: vec}
}

// With creates new or returns existing counter with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *CounterVec) With(tags map[string]string) metrics.Counter {
	return v.vec.with(tags).(*Counter)
}

// Reset deletes all metrics in this vector.
func (v *CounterVec) Reset() {
	v.vec.reset()
}

var _ metrics.GaugeVec = (*GaugeVec)(nil)

// GaugeVec stores gauges and
// implements metrics.GaugeVec interface.
type GaugeVec struct {
	vec *metricsVector
}

// GaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) GaugeVec(name string, labels []string) metrics.GaugeVec {
	return &GaugeVec{
		vec: &metricsVector{
			labels:  append([]string(nil), labels...),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).Gauge(name).(*Gauge)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *GaugeVec) With(tags map[string]string) metrics.Gauge {
	return v.vec.with(tags).(*Gauge)
}

// Reset deletes all metrics in this vector.
func (v *GaugeVec) Reset() {
	v.vec.reset()
}

var _ metrics.TimerVec = (*TimerVec)(nil)

// TimerVec stores timers and
// implements metrics.TimerVec interface.
type TimerVec struct {
	vec *metricsVector
}

// TimerVec creates a new timers vector with given metric name and
// partitioned by the given label names.
func (r *Registry) TimerVec(name string, labels []string) metrics.TimerVec {
	return &TimerVec{
		vec: &metricsVector{
			labels:  append([]string(nil), labels...),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).Timer(name).(*Timer)
			},
		},
	}
}

// With creates new or returns existing timer with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *TimerVec) With(tags map[string]string) metrics.Timer {
	return v.vec.with(tags).(*Timer)
}

// Reset deletes all metrics in this vector.
func (v *TimerVec) Reset() {
	v.vec.reset()
}

var _ metrics.HistogramVec = (*HistogramVec)(nil)

// HistogramVec stores histograms and
// implements metrics.HistogramVec interface.
type HistogramVec struct {
	vec *metricsVector
}

// HistogramVec creates a new histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	var vec *metricsVector
	vec = &metricsVector{
		labels:  append([]string(nil), labels...),
		metrics: make(map[uint64]Metric),
		rated:   r.rated,
		newMetric: func(tags map[string]string) Metric {
			return r.Rated(vec.rated).
				WithTags(tags).
				Histogram(name, buckets).(*Histogram)
		},
	}
	return &HistogramVec{vec: vec}
}

// With creates new or returns existing histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *HistogramVec) With(tags map[string]string) metrics.Histogram {
	return v.vec.with(tags).(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *HistogramVec) Reset() {
	v.vec.reset()
}

var _ metrics.TimerVec = (*DurationHistogramVec)(nil)

// DurationHistogramVec stores duration histograms and
// implements metrics.TimerVec interface.
type DurationHistogramVec struct {
	vec *metricsVector
}

// DurationHistogramVec creates a new duration histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
	var vec *metricsVector
	vec = &metricsVector{
		labels:  append([]string(nil), labels...),
		metrics: make(map[uint64]Metric),
		rated:   r.rated,
		newMetric: func(tags map[string]string) Metric {
			return r.Rated(vec.rated).
				WithTags(tags).
				DurationHistogram(name, buckets).(*Histogram)
		},
	}
	return &DurationHistogramVec{vec: vec}
}

// With creates new or returns existing duration histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *DurationHistogramVec) With(tags map[string]string) metrics.Timer {
	return v.vec.with(tags).(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *DurationHistogramVec) Reset() {
	v.vec.reset()
}
