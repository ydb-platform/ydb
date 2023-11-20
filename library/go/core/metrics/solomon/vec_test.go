package solomon

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

func TestVec(t *testing.T) {
	type args struct {
		name     string
		labels   []string
		buckets  metrics.Buckets
		dbuckets metrics.DurationBuckets
	}

	testCases := []struct {
		name         string
		args         args
		expectedType interface{}
		expectLabels []string
	}{
		{
			name: "CounterVec",
			args: args{
				name:   "cntvec",
				labels: []string{"shimba", "looken"},
			},
			expectedType: &CounterVec{},
			expectLabels: []string{"shimba", "looken"},
		},
		{
			name: "GaugeVec",
			args: args{
				name:   "ggvec",
				labels: []string{"shimba", "looken"},
			},
			expectedType: &GaugeVec{},
			expectLabels: []string{"shimba", "looken"},
		},
		{
			name: "TimerVec",
			args: args{
				name:   "tvec",
				labels: []string{"shimba", "looken"},
			},
			expectedType: &TimerVec{},
			expectLabels: []string{"shimba", "looken"},
		},
		{
			name: "HistogramVec",
			args: args{
				name:    "hvec",
				labels:  []string{"shimba", "looken"},
				buckets: metrics.NewBuckets(1, 2, 3, 4),
			},
			expectedType: &HistogramVec{},
			expectLabels: []string{"shimba", "looken"},
		},
		{
			name: "DurationHistogramVec",
			args: args{
				name:     "dhvec",
				labels:   []string{"shimba", "looken"},
				dbuckets: metrics.NewDurationBuckets(1, 2, 3, 4),
			},
			expectedType: &DurationHistogramVec{},
			expectLabels: []string{"shimba", "looken"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rg := NewRegistry(NewRegistryOpts())

			switch vect := tc.expectedType.(type) {
			case *CounterVec:
				vec := rg.CounterVec(tc.args.name, tc.args.labels)
				assert.IsType(t, vect, vec)
				assert.Equal(t, tc.expectLabels, vec.(*CounterVec).vec.labels)
			case *GaugeVec:
				vec := rg.GaugeVec(tc.args.name, tc.args.labels)
				assert.IsType(t, vect, vec)
				assert.Equal(t, tc.expectLabels, vec.(*GaugeVec).vec.labels)
			case *TimerVec:
				vec := rg.TimerVec(tc.args.name, tc.args.labels)
				assert.IsType(t, vect, vec)
				assert.Equal(t, tc.expectLabels, vec.(*TimerVec).vec.labels)
			case *HistogramVec:
				vec := rg.HistogramVec(tc.args.name, tc.args.buckets, tc.args.labels)
				assert.IsType(t, vect, vec)
				assert.Equal(t, tc.expectLabels, vec.(*HistogramVec).vec.labels)
			case *DurationHistogramVec:
				vec := rg.DurationHistogramVec(tc.args.name, tc.args.dbuckets, tc.args.labels)
				assert.IsType(t, vect, vec)
				assert.Equal(t, tc.expectLabels, vec.(*DurationHistogramVec).vec.labels)
			default:
				t.Errorf("unknown type: %T", vect)
			}
		})
	}
}

func TestCounterVecWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	t.Run("plain", func(t *testing.T) {
		vec := rg.CounterVec("ololo", []string{"shimba", "looken"})
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &CounterVec{}, vec)
		assert.IsType(t, &Counter{}, metric)
		assert.Equal(t, typeCounter, metric.(*Counter).metricType)

		assert.NotEmpty(t, vec.(*CounterVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*CounterVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Counter))
	})

	t.Run("rated", func(t *testing.T) {
		vec := rg.CounterVec("ololo", []string{"shimba", "looken"})
		Rated(vec)
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &CounterVec{}, vec)
		assert.IsType(t, &Counter{}, metric)
		assert.Equal(t, typeRated, metric.(*Counter).metricType)

		assert.NotEmpty(t, vec.(*CounterVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*CounterVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Counter))
	})
}

func TestGaugeVecWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	vec := rg.GaugeVec("ololo", []string{"shimba", "looken"})
	tags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(tags)

	assert.IsType(t, &GaugeVec{}, vec)
	assert.IsType(t, &Gauge{}, metric)
	assert.Equal(t, typeGauge, metric.(*Gauge).metricType)

	assert.NotEmpty(t, vec.(*GaugeVec).vec.metrics)
	vec.Reset()
	assert.Empty(t, vec.(*GaugeVec).vec.metrics)
	assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Gauge))
}

func TestTimerVecWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec := rg.TimerVec("ololo", []string{"shimba", "looken"})
	tags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}
	metric := vec.With(tags)

	assert.IsType(t, &TimerVec{}, vec)
	assert.IsType(t, &Timer{}, metric)
	assert.Equal(t, typeGauge, metric.(*Timer).metricType)

	assert.NotEmpty(t, vec.(*TimerVec).vec.metrics)
	vec.Reset()
	assert.Empty(t, vec.(*TimerVec).vec.metrics)
	assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Timer))
}

func TestHistogramVecWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	t.Run("plain", func(t *testing.T) {
		buckets := metrics.NewBuckets(1, 2, 3)
		vec := rg.HistogramVec("ololo", buckets, []string{"shimba", "looken"})
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &HistogramVec{}, vec)
		assert.IsType(t, &Histogram{}, metric)
		assert.Equal(t, typeHistogram, metric.(*Histogram).metricType)

		assert.NotEmpty(t, vec.(*HistogramVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*HistogramVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Histogram))
	})

	t.Run("rated", func(t *testing.T) {
		buckets := metrics.NewBuckets(1, 2, 3)
		vec := rg.HistogramVec("ololo", buckets, []string{"shimba", "looken"})
		Rated(vec)
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &HistogramVec{}, vec)
		assert.IsType(t, &Histogram{}, metric)
		assert.Equal(t, typeRatedHistogram, metric.(*Histogram).metricType)

		assert.NotEmpty(t, vec.(*HistogramVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*HistogramVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Histogram))
	})
}

func TestDurationHistogramVecWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	t.Run("plain", func(t *testing.T) {
		buckets := metrics.NewDurationBuckets(1, 2, 3)
		vec := rg.DurationHistogramVec("ololo", buckets, []string{"shimba", "looken"})
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &DurationHistogramVec{}, vec)
		assert.IsType(t, &Histogram{}, metric)
		assert.Equal(t, typeHistogram, metric.(*Histogram).metricType)

		assert.NotEmpty(t, vec.(*DurationHistogramVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*DurationHistogramVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Histogram))
	})

	t.Run("rated", func(t *testing.T) {
		buckets := metrics.NewDurationBuckets(1, 2, 3)
		vec := rg.DurationHistogramVec("ololo", buckets, []string{"shimba", "looken"})
		Rated(vec)
		tags := map[string]string{
			"shimba": "boomba",
			"looken": "tooken",
		}
		metric := vec.With(tags)

		assert.IsType(t, &DurationHistogramVec{}, vec)
		assert.IsType(t, &Histogram{}, metric)
		assert.Equal(t, typeRatedHistogram, metric.(*Histogram).metricType)

		assert.NotEmpty(t, vec.(*DurationHistogramVec).vec.metrics)
		vec.Reset()
		assert.Empty(t, vec.(*DurationHistogramVec).vec.metrics)
		assertMetricRemoved(t, rg.WithTags(tags).(*Registry), metric.(*Histogram))
	})
}

func TestMetricsVectorWith(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	name := "ololo"
	tags := map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	}

	vec := &metricsVector{
		labels:  []string{"shimba", "looken"},
		metrics: make(map[uint64]Metric),
		newMetric: func(tags map[string]string) Metric {
			return rg.WithTags(tags).Counter(name).(*Counter)
		},
		removeMetric: func(m Metric) {
			rg.WithTags(m.getLabels()).(*Registry).unregisterMetric(m)
		},
	}

	// check first counter
	metric := vec.with(tags)
	require.IsType(t, &Counter{}, metric)
	cnt := metric.(*Counter)
	assert.Equal(t, name, cnt.name)
	assert.Equal(t, tags, cnt.tags)

	// check vector length
	assert.Equal(t, 1, len(vec.metrics))

	// check same counter returned for same tags set
	cnt2 := vec.with(tags)
	assert.Same(t, cnt, cnt2)

	// check vector length
	assert.Equal(t, 1, len(vec.metrics))

	// return new counter
	cnt3 := vec.with(map[string]string{
		"shimba": "boomba",
		"looken": "cooken",
	})
	assert.NotSame(t, cnt, cnt3)

	// check vector length
	assert.Equal(t, 2, len(vec.metrics))

	// check for panic
	assert.Panics(t, func() {
		vec.with(map[string]string{"chicken": "cooken"})
	})
	assert.Panics(t, func() {
		vec.with(map[string]string{"shimba": "boomba", "chicken": "cooken"})
	})

	// check reset
	vec.reset()
	assert.Empty(t, vec.metrics)
	assertMetricRemoved(t, rg.WithTags(tags).(*Registry), cnt2)
	assertMetricRemoved(t, rg.WithTags(tags).(*Registry), cnt3)
}

func assertMetricRemoved(t *testing.T, rg *Registry, m Metric) {
	t.Helper()

	v, ok := rg.metrics.Load(rg.metricKey(m))
	assert.False(t, ok, v)
}
