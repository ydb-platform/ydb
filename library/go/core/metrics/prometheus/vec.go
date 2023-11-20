package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/metricsutil"
	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

var _ metrics.CounterVec = (*CounterVec)(nil)

// CounterVec wraps prometheus.CounterVec
// and implements metrics.CounterVec interface.
type CounterVec struct {
	vec *prometheus.CounterVec
}

// CounterVec creates a new counters vector with given metric name and
// partitioned by the given label names.
func (r *Registry) CounterVec(name string, labels []string) metrics.CounterVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &CounterVec{vec: existErr.ExistingCollector.(*prometheus.CounterVec)}
		}
		panic(err)
	}

	return &CounterVec{vec: vec}
}

// With creates new or returns existing counter with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *CounterVec) With(tags map[string]string) metrics.Counter {
	return &Counter{cnt: v.vec.With(tags)}
}

// Reset deletes all metrics in this vector.
func (v *CounterVec) Reset() {
	v.vec.Reset()
}

var _ metrics.GaugeVec = (*GaugeVec)(nil)

// GaugeVec wraps prometheus.GaugeVec
// and implements metrics.GaugeVec interface.
type GaugeVec struct {
	vec *prometheus.GaugeVec
}

// GaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) GaugeVec(name string, labels []string) metrics.GaugeVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &GaugeVec{vec: existErr.ExistingCollector.(*prometheus.GaugeVec)}
		}
		panic(err)
	}

	return &GaugeVec{vec: vec}
}

// With creates new or returns existing gauge with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *GaugeVec) With(tags map[string]string) metrics.Gauge {
	return &Gauge{gg: v.vec.With(tags)}
}

// Reset deletes all metrics in this vector.
func (v *GaugeVec) Reset() {
	v.vec.Reset()
}

// IntGaugeVec wraps prometheus.GaugeVec
// and implements metrics.IntGaugeVec interface.
type IntGaugeVec struct {
	vec *prometheus.GaugeVec
}

// IntGaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) IntGaugeVec(name string, labels []string) metrics.IntGaugeVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &IntGaugeVec{vec: existErr.ExistingCollector.(*prometheus.GaugeVec)}
		}
		panic(err)
	}

	return &IntGaugeVec{vec: vec}
}

// With creates new or returns existing gauge with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *IntGaugeVec) With(tags map[string]string) metrics.IntGauge {
	return &IntGauge{Gauge{gg: v.vec.With(tags)}}
}

// Reset deletes all metrics in this vector.
func (v *IntGaugeVec) Reset() {
	v.vec.Reset()
}

var _ metrics.TimerVec = (*TimerVec)(nil)

// TimerVec wraps prometheus.GaugeVec
// and implements metrics.TimerVec interface.
type TimerVec struct {
	vec *prometheus.GaugeVec
}

// TimerVec creates a new timers vector with given metric name and
// partitioned by the given label names.
func (r *Registry) TimerVec(name string, labels []string) metrics.TimerVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &TimerVec{vec: existErr.ExistingCollector.(*prometheus.GaugeVec)}
		}
		panic(err)
	}

	return &TimerVec{vec: vec}
}

// With creates new or returns existing timer with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *TimerVec) With(tags map[string]string) metrics.Timer {
	return &Timer{gg: v.vec.With(tags)}
}

// Reset deletes all metrics in this vector.
func (v *TimerVec) Reset() {
	v.vec.Reset()
}

var _ metrics.HistogramVec = (*HistogramVec)(nil)

// HistogramVec wraps prometheus.HistogramVec
// and implements metrics.HistogramVec interface.
type HistogramVec struct {
	vec *prometheus.HistogramVec
}

// HistogramVec creates a new histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
		Buckets:     metricsutil.BucketsBounds(buckets),
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &HistogramVec{vec: existErr.ExistingCollector.(*prometheus.HistogramVec)}
		}
		panic(err)
	}

	return &HistogramVec{vec: vec}
}

// With creates new or returns existing histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *HistogramVec) With(tags map[string]string) metrics.Histogram {
	return &Histogram{hm: v.vec.With(tags)}
}

// Reset deletes all metrics in this vector.
func (v *HistogramVec) Reset() {
	v.vec.Reset()
}

var _ metrics.TimerVec = (*DurationHistogramVec)(nil)

// DurationHistogramVec wraps prometheus.HistogramVec
// and implements metrics.TimerVec interface.
type DurationHistogramVec struct {
	vec *prometheus.HistogramVec
}

// DurationHistogramVec creates a new duration histograms vector with given metric name and buckets and
// partitioned by the given label names.
func (r *Registry) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
	name = r.sanitizeName(name)
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
		Buckets:     metricsutil.DurationBucketsBounds(buckets),
	}, labels)

	if err := r.rg.Register(vec); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &DurationHistogramVec{vec: existErr.ExistingCollector.(*prometheus.HistogramVec)}
		}
		panic(err)
	}

	return &DurationHistogramVec{vec: vec}
}

// With creates new or returns existing duration histogram with given tags from vector.
// It will panic if tags keys set is not equal to vector labels.
func (v *DurationHistogramVec) With(tags map[string]string) metrics.Timer {
	return &Histogram{hm: v.vec.With(tags)}
}

// Reset deletes all metrics in this vector.
func (v *DurationHistogramVec) Reset() {
	v.vec.Reset()
}
