package prometheus

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/metricsutil"
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/registryutil"
	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

var _ metrics.Registry = (*Registry)(nil)
var _ metrics.MetricsStreamer = (*Registry)(nil)

type Registry struct {
	rg *prometheus.Registry

	m             *sync.Mutex
	subregistries map[string]*Registry

	tags          map[string]string
	prefix        string
	nameSanitizer func(string) string
	streamFormat  expfmt.Format
}

// NewRegistry creates new Prometheus backed registry.
func NewRegistry(opts *RegistryOpts) *Registry {
	r := &Registry{
		rg:            prometheus.NewRegistry(),
		m:             new(sync.Mutex),
		subregistries: make(map[string]*Registry),
		tags:          make(map[string]string),
		streamFormat:  StreamCompact,
	}

	if opts != nil {
		r.prefix = opts.Prefix
		r.tags = opts.Tags
		r.streamFormat = opts.StreamFormat
		if opts.rg != nil {
			r.rg = opts.rg
		}
		for _, collector := range opts.Collectors {
			collector(r)
		}
		if opts.NameSanitizer != nil {
			r.nameSanitizer = opts.NameSanitizer
		}
	}

	return r
}

// WithTags creates new sub-scope, where each metric has tags attached to it.
func (r Registry) WithTags(tags map[string]string) metrics.Registry {
	return r.newSubregistry(r.prefix, registryutil.MergeTags(r.tags, tags))
}

// WithPrefix creates new sub-scope, where each metric has prefix added to it name.
func (r Registry) WithPrefix(prefix string) metrics.Registry {
	return r.newSubregistry(registryutil.BuildFQName("_", r.prefix, prefix), r.tags)
}

// ComposeName builds FQ name with appropriate separator.
func (r Registry) ComposeName(parts ...string) string {
	return registryutil.BuildFQName("_", parts...)
}

func (r Registry) Counter(name string) metrics.Counter {
	name = r.sanitizeName(name)
	cnt := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	})

	if err := r.rg.Register(cnt); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &Counter{cnt: existErr.ExistingCollector.(prometheus.Counter)}
		}
		panic(err)
	}

	return &Counter{cnt: cnt}
}

func (r Registry) FuncCounter(name string, function func() int64) metrics.FuncCounter {
	name = r.sanitizeName(name)
	cnt := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, func() float64 {
		return float64(function())
	})

	if err := r.rg.Register(cnt); err != nil {
		panic(err)
	}

	return &FuncCounter{
		cnt:      cnt,
		function: function,
	}
}

func (r Registry) Gauge(name string) metrics.Gauge {
	name = r.sanitizeName(name)
	gg := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	})

	if err := r.rg.Register(gg); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &Gauge{gg: existErr.ExistingCollector.(prometheus.Gauge)}
		}
		panic(err)
	}

	return &Gauge{gg: gg}
}

func (r Registry) FuncGauge(name string, function func() float64) metrics.FuncGauge {
	name = r.sanitizeName(name)
	ff := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, function)
	if err := r.rg.Register(ff); err != nil {
		panic(err)
	}
	return &FuncGauge{
		ff:       ff,
		function: function,
	}
}

func (r Registry) IntGauge(name string) metrics.IntGauge {
	return &IntGauge{Gauge: r.Gauge(name)}
}

func (r Registry) FuncIntGauge(name string, function func() int64) metrics.FuncIntGauge {
	name = r.sanitizeName(name)
	ff := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	}, func() float64 { return float64(function()) })
	if err := r.rg.Register(ff); err != nil {
		panic(err)
	}
	return &FuncIntGauge{
		ff:       ff,
		function: function,
	}
}

func (r Registry) Timer(name string) metrics.Timer {
	name = r.sanitizeName(name)
	gg := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
	})

	if err := r.rg.Register(gg); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &Timer{gg: existErr.ExistingCollector.(prometheus.Gauge)}
		}
		panic(err)
	}

	return &Timer{gg: gg}
}

func (r Registry) Histogram(name string, buckets metrics.Buckets) metrics.Histogram {
	name = r.sanitizeName(name)
	hm := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
		Buckets:     metricsutil.BucketsBounds(buckets),
	})

	if err := r.rg.Register(hm); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &Histogram{hm: existErr.ExistingCollector.(prometheus.Observer)}
		}
		panic(err)
	}

	return &Histogram{hm: hm}
}

func (r Registry) DurationHistogram(name string, buckets metrics.DurationBuckets) metrics.Timer {
	name = r.sanitizeName(name)
	hm := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   r.prefix,
		Name:        name,
		ConstLabels: r.tags,
		Buckets:     metricsutil.DurationBucketsBounds(buckets),
	})

	if err := r.rg.Register(hm); err != nil {
		var existErr prometheus.AlreadyRegisteredError
		if xerrors.As(err, &existErr) {
			return &Histogram{hm: existErr.ExistingCollector.(prometheus.Histogram)}
		}
		panic(err)
	}

	return &Histogram{hm: hm}
}

// Gather returns raw collected Prometheus metrics.
func (r Registry) Gather() ([]*dto.MetricFamily, error) {
	return r.rg.Gather()
}

func (r *Registry) newSubregistry(prefix string, tags map[string]string) *Registry {
	registryKey := registryutil.BuildRegistryKey(prefix, tags)

	r.m.Lock()
	defer r.m.Unlock()

	if old, ok := r.subregistries[registryKey]; ok {
		return old
	}

	subregistry := &Registry{
		rg:            r.rg,
		m:             r.m,
		subregistries: r.subregistries,
		tags:          tags,
		prefix:        prefix,
		nameSanitizer: r.nameSanitizer,
	}

	r.subregistries[registryKey] = subregistry
	return subregistry
}

func (r *Registry) sanitizeName(name string) string {
	if r.nameSanitizer == nil {
		return name
	}
	return r.nameSanitizer(name)
}
