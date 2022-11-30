package solomon

import (
	"reflect"
	"strconv"
	"sync"

	"a.yandex-team.ru/library/go/core/metrics"
	"a.yandex-team.ru/library/go/core/metrics/internal/pkg/metricsutil"
	"a.yandex-team.ru/library/go/core/metrics/internal/pkg/registryutil"
)

var _ metrics.Registry = (*Registry)(nil)

type Registry struct {
	separator  string
	prefix     string
	tags       map[string]string
	rated      bool
	useNameTag bool

	subregistries map[string]*Registry
	m             *sync.Mutex

	metrics *sync.Map
}

func NewRegistry(opts *RegistryOpts) *Registry {
	r := &Registry{
		separator:  ".",
		useNameTag: false,

		subregistries: make(map[string]*Registry),
		m:             new(sync.Mutex),

		metrics: new(sync.Map),
	}

	if opts != nil {
		r.separator = string(opts.Separator)
		r.prefix = opts.Prefix
		r.tags = opts.Tags
		r.rated = opts.Rated
		r.useNameTag = opts.UseNameTag
		for _, collector := range opts.Collectors {
			collector(r)
		}
	}

	return r
}

// Rated returns copy of registry with rated set to desired value.
func (r Registry) Rated(rated bool) metrics.Registry {
	return &Registry{
		separator:  r.separator,
		prefix:     r.prefix,
		tags:       r.tags,
		rated:      rated,
		useNameTag: r.useNameTag,

		subregistries: r.subregistries,
		m:             r.m,

		metrics: r.metrics,
	}
}

// WithTags creates new sub-scope, where each metric has tags attached to it.
func (r Registry) WithTags(tags map[string]string) metrics.Registry {
	return r.newSubregistry(r.prefix, registryutil.MergeTags(r.tags, tags))
}

// WithPrefix creates new sub-scope, where each metric has prefix added to it name.
func (r Registry) WithPrefix(prefix string) metrics.Registry {
	return r.newSubregistry(registryutil.BuildFQName(r.separator, r.prefix, prefix), r.tags)
}

// ComposeName builds FQ name with appropriate separator.
func (r Registry) ComposeName(parts ...string) string {
	return registryutil.BuildFQName(r.separator, parts...)
}

func (r Registry) Counter(name string) metrics.Counter {
	s := &Counter{
		name:       r.newMetricName(name),
		metricType: typeCounter,
		tags:       r.tags,

		useNameTag: r.useNameTag,
	}

	return r.registerMetric(s).(metrics.Counter)
}

func (r Registry) FuncCounter(name string, function func() int64) metrics.FuncCounter {
	s := &FuncCounter{
		name:       r.newMetricName(name),
		metricType: typeCounter,
		tags:       r.tags,
		function:   function,
		useNameTag: r.useNameTag,
	}

	return r.registerMetric(s).(metrics.FuncCounter)
}

func (r Registry) Gauge(name string) metrics.Gauge {
	s := &Gauge{
		name:       r.newMetricName(name),
		metricType: typeGauge,
		tags:       r.tags,
		useNameTag: r.useNameTag,
	}

	return r.registerMetric(s).(metrics.Gauge)
}

func (r Registry) FuncGauge(name string, function func() float64) metrics.FuncGauge {
	s := &FuncGauge{
		name:       r.newMetricName(name),
		metricType: typeGauge,
		tags:       r.tags,
		function:   function,
		useNameTag: r.useNameTag,
	}

	return r.registerMetric(s).(metrics.FuncGauge)
}

func (r Registry) Timer(name string) metrics.Timer {
	s := &Timer{
		name:       r.newMetricName(name),
		metricType: typeGauge,
		tags:       r.tags,
		useNameTag: r.useNameTag,
	}

	return r.registerMetric(s).(metrics.Timer)
}

func (r Registry) Histogram(name string, buckets metrics.Buckets) metrics.Histogram {
	s := &Histogram{
		name:         r.newMetricName(name),
		metricType:   typeHistogram,
		tags:         r.tags,
		bucketBounds: metricsutil.BucketsBounds(buckets),
		bucketValues: make([]int64, buckets.Size()),
		useNameTag:   r.useNameTag,
	}

	return r.registerMetric(s).(metrics.Histogram)
}

func (r Registry) DurationHistogram(name string, buckets metrics.DurationBuckets) metrics.Timer {
	s := &Histogram{
		name:         r.newMetricName(name),
		metricType:   typeHistogram,
		tags:         r.tags,
		bucketBounds: metricsutil.DurationBucketsBounds(buckets),
		bucketValues: make([]int64, buckets.Size()),
		useNameTag:   r.useNameTag,
	}

	return r.registerMetric(s).(metrics.Timer)
}

func (r *Registry) newSubregistry(prefix string, tags map[string]string) *Registry {
	// differ simple and rated registries
	keyTags := registryutil.MergeTags(tags, map[string]string{"rated": strconv.FormatBool(r.rated)})
	registryKey := registryutil.BuildRegistryKey(prefix, keyTags)

	r.m.Lock()
	defer r.m.Unlock()

	if existing, ok := r.subregistries[registryKey]; ok {
		return existing
	}

	subregistry := &Registry{
		separator:  r.separator,
		prefix:     prefix,
		tags:       tags,
		rated:      r.rated,
		useNameTag: r.useNameTag,

		subregistries: r.subregistries,
		m:             r.m,

		metrics: r.metrics,
	}

	r.subregistries[registryKey] = subregistry
	return subregistry
}

func (r *Registry) newMetricName(name string) string {
	return registryutil.BuildFQName(r.separator, r.prefix, name)
}

func (r *Registry) registerMetric(s Metric) Metric {
	if r.rated {
		Rated(s)
	}

	// differ simple and rated registries
	keyTags := registryutil.MergeTags(r.tags, map[string]string{"rated": strconv.FormatBool(r.rated)})
	key := registryutil.BuildRegistryKey(s.Name(), keyTags)

	oldMetric, loaded := r.metrics.LoadOrStore(key, s)
	if !loaded {
		return s
	}

	if reflect.TypeOf(oldMetric) == reflect.TypeOf(s) {
		return oldMetric.(Metric)
	} else {
		r.metrics.Store(key, s)
		return s
	}
}
