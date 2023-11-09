package prometheus

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/ptr"
	"github.com/ydb-platform/ydb/library/go/test/assertpb"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNewRegistry(t *testing.T) {
	expected := &Registry{
		rg:            prometheus.NewRegistry(),
		m:             new(sync.Mutex),
		subregistries: make(map[string]*Registry),
		tags:          map[string]string{},
		prefix:        "",
	}

	r := NewRegistry(nil)
	assert.IsType(t, expected, r)
	assert.Equal(t, expected, r)
}

func TestRegistry_Subregisters(t *testing.T) {
	r := NewRegistry(nil)
	sr1 := r.WithPrefix("subregister1").
		WithTags(map[string]string{"ololo": "trololo"})
	sr2 := sr1.WithPrefix("subregister2").
		WithTags(map[string]string{"shimba": "boomba"})

	// check global subregistries map
	expectedMap := map[string]*Registry{
		"\"subregister1\"{}": {
			rg:            r.rg,
			m:             r.m,
			subregistries: r.subregistries,
			prefix:        "subregister1",
			tags:          make(map[string]string),
		},
		"\"subregister1\"{ololo=trololo}": {
			rg:            r.rg,
			m:             r.m,
			subregistries: r.subregistries,
			tags:          map[string]string{"ololo": "trololo"},
			prefix:        "subregister1",
		},
		"\"subregister1_subregister2\"{ololo=trololo}": {
			rg:            r.rg,
			m:             r.m,
			subregistries: r.subregistries,
			tags:          map[string]string{"ololo": "trololo"},
			prefix:        "subregister1_subregister2",
		},
		"\"subregister1_subregister2\"{ololo=trololo,shimba=boomba}": {
			rg:            r.rg,
			m:             r.m,
			subregistries: r.subregistries,
			tags:          map[string]string{"ololo": "trololo", "shimba": "boomba"},
			prefix:        "subregister1_subregister2",
		},
	}

	assert.EqualValues(t, expectedMap, r.subregistries)

	// top-register write
	rCnt := r.Counter("subregisters_count")
	rCnt.Add(2)

	// sub-register write
	srTm := sr1.Timer("mytimer")
	srTm.RecordDuration(42 * time.Second)

	// sub-sub-register write
	srHm := sr2.Histogram("myhistogram", metrics.NewBuckets(1, 2, 3))
	srHm.RecordValue(1.5)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("subregister1_mytimer"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_GAUGE),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
					},
					Gauge: &dto.Gauge{Value: ptr.Float64(42)},
				},
			},
		},
		{
			Name: ptr.String("subregister1_subregister2_myhistogram"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_HISTOGRAM),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
						{Name: ptr.String("shimba"), Value: ptr.String("boomba")},
					},
					Histogram: &dto.Histogram{
						SampleCount: ptr.Uint64(1),
						SampleSum:   ptr.Float64(1.5),
						Bucket: []*dto.Bucket{
							{CumulativeCount: ptr.Uint64(0), UpperBound: ptr.Float64(1)},
							{CumulativeCount: ptr.Uint64(1), UpperBound: ptr.Float64(2)},
							{CumulativeCount: ptr.Uint64(1), UpperBound: ptr.Float64(3)},
						},
					},
				},
			},
		},
		{
			Name: ptr.String("subregisters_count"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_COUNTER),
			Metric: []*dto.Metric{
				{
					Label:   []*dto.LabelPair{},
					Counter: &dto.Counter{Value: ptr.Float64(2)},
				},
			},
		},
	}

	cmpOpts := []cmp.Option{
		protocmp.Transform(),
	}
	assert.True(t, cmp.Equal(expected, mr, cmpOpts...), cmp.Diff(expected, mr, cmpOpts...))
}

func TestRegistry_Counter(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})

	// must panic on empty name
	assert.Panics(t, func() { r.Counter("") })

	srCnt := sr.Counter("mycounter")
	srCnt.Add(42)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_mycounter"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_COUNTER),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
					},
					Counter: &dto.Counter{Value: ptr.Float64(42)},
				},
			},
		},
	}
	cmpOpts := []cmp.Option{
		protocmp.Transform(),
	}
	assert.True(t, cmp.Equal(expected, mr, cmpOpts...), cmp.Diff(expected, mr, cmpOpts...))
}

func TestRegistry_DurationHistogram(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})

	// must panic on empty name
	assert.Panics(t, func() { r.DurationHistogram("", nil) })

	cnt := sr.DurationHistogram("myhistogram", metrics.NewDurationBuckets(
		1*time.Second, 3*time.Second, 5*time.Second,
	))

	cnt.RecordDuration(2 * time.Second)
	cnt.RecordDuration(4 * time.Second)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_myhistogram"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_HISTOGRAM),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{{Name: ptr.String("ololo"), Value: ptr.String("trololo")}},
					Histogram: &dto.Histogram{
						SampleCount: ptr.Uint64(2),
						SampleSum:   ptr.Float64(6),
						Bucket: []*dto.Bucket{
							{CumulativeCount: ptr.Uint64(0), UpperBound: ptr.Float64(1)},
							{CumulativeCount: ptr.Uint64(1), UpperBound: ptr.Float64(3)},
							{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(5)},
						},
					},
				},
			},
		},
	}
	assertpb.Equal(t, expected, mr)
}

func TestRegistry_Gauge(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})

	// must panic on empty name
	assert.Panics(t, func() { r.Gauge("") })

	cnt := sr.Gauge("mygauge")
	cnt.Add(42)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_mygauge"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_GAUGE),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{{Name: ptr.String("ololo"), Value: ptr.String("trololo")}},
					Gauge: &dto.Gauge{Value: ptr.Float64(42)},
				},
			},
		},
	}
	assertpb.Equal(t, expected, mr)
}

func TestRegistry_Histogram(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})

	// must panic on empty name
	assert.Panics(t, func() { r.Histogram("", nil) })

	cnt := sr.Histogram("myhistogram", metrics.NewBuckets(1, 3, 5))

	cnt.RecordValue(2)
	cnt.RecordValue(4)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_myhistogram"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_HISTOGRAM),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{{Name: ptr.String("ololo"), Value: ptr.String("trololo")}},
					Histogram: &dto.Histogram{
						SampleCount: ptr.Uint64(2),
						SampleSum:   ptr.Float64(6),
						Bucket: []*dto.Bucket{
							{CumulativeCount: ptr.Uint64(0), UpperBound: ptr.Float64(1)},
							{CumulativeCount: ptr.Uint64(1), UpperBound: ptr.Float64(3)},
							{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(5)},
						},
					},
				},
			},
		},
	}
	assertpb.Equal(t, expected, mr)
}

func TestRegistry_Timer(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})

	// must panic on empty name
	assert.Panics(t, func() { r.Timer("") })

	cnt := sr.Timer("mytimer")
	cnt.RecordDuration(42 * time.Second)

	mr, err := r.Gather()
	assert.NoError(t, err)

	assert.IsType(t, mr, []*dto.MetricFamily{})

	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_mytimer"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_GAUGE),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{{Name: ptr.String("ololo"), Value: ptr.String("trololo")}},
					Gauge: &dto.Gauge{Value: ptr.Float64(42)},
				},
			},
		},
	}
	assertpb.Equal(t, expected, mr)
}

func TestRegistry_WithPrefix(t *testing.T) {
	testCases := []struct {
		r        metrics.Registry
		expected string
	}{
		{
			r: func() metrics.Registry {
				return NewRegistry(nil)
			}(),
			expected: "",
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).WithPrefix("myprefix")
			}(),
			expected: "myprefix",
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).WithPrefix("__myprefix_")
			}(),
			expected: "myprefix",
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).WithPrefix("__myprefix_").WithPrefix("mysubprefix______")
			}(),
			expected: "myprefix_mysubprefix",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.r.(*Registry).prefix)
		})
	}
}

func TestRegistry_WithTags(t *testing.T) {
	testCases := []struct {
		r        metrics.Registry
		expected map[string]string
	}{
		{
			r: func() metrics.Registry {
				return NewRegistry(nil)
			}(),
			expected: map[string]string{},
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).WithTags(map[string]string{"shimba": "boomba"})
			}(),
			expected: map[string]string{"shimba": "boomba"},
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).
					WithTags(map[string]string{"shimba": "boomba"}).
					WithTags(map[string]string{"looken": "tooken"})
			}(),
			expected: map[string]string{
				"shimba": "boomba",
				"looken": "tooken",
			},
		},
		{
			r: func() metrics.Registry {
				return NewRegistry(nil).
					WithTags(map[string]string{"shimba": "boomba"}).
					WithTags(map[string]string{"looken": "tooken"}).
					WithTags(map[string]string{"shimba": "cooken"})
			}(),
			expected: map[string]string{
				"shimba": "cooken",
				"looken": "tooken",
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.r.(*Registry).tags)
		})
	}
}

func TestRegistry_WithTags_NoPanic(t *testing.T) {
	_ = NewRegistry(nil).WithTags(map[string]string{"foo": "bar"})
	_ = NewRegistry(nil).WithTags(map[string]string{"foo": "bar"})
}

func TestRegistry_Counter_NoPanic(t *testing.T) {
	r := NewRegistry(nil)
	sr := r.WithPrefix("myprefix").
		WithTags(map[string]string{"ololo": "trololo"})
	cntrRaz := sr.Counter("mycounter").(*Counter)
	cntrDvaz := sr.Counter("mycounter").(*Counter)
	assert.Equal(t, cntrRaz.cnt, cntrDvaz.cnt)
	cntrRaz.Add(100)
	cntrDvaz.Add(100)
	mr, err := r.Gather()
	assert.NoError(t, err)
	expected := []*dto.MetricFamily{
		{
			Name: ptr.String("myprefix_mycounter"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_COUNTER),
			Metric: []*dto.Metric{
				{
					Label:   []*dto.LabelPair{{Name: ptr.String("ololo"), Value: ptr.String("trololo")}},
					Counter: &dto.Counter{Value: ptr.Float64(200)},
				},
			},
		},
	}
	assertpb.Equal(t, expected, mr)
}

func TestRegistry_NameSanitizer(t *testing.T) {
	testCases := []struct {
		opts *RegistryOpts
		name string
		want string
	}{
		{
			opts: nil,
			name: "some_name",
			want: "some_name",
		},
		{
			opts: NewRegistryOpts().SetNameSanitizer(func(s string) string {
				return strings.ReplaceAll(s, "/", "_")
			}),
			name: "other/name",
			want: "other_name",
		},
	}

	for _, tc := range testCases {
		r := NewRegistry(tc.opts)
		_ = r.Counter(tc.name)
		mfs, err := r.Gather()
		assert.NoError(t, err)
		assert.NotEmpty(t, mfs)

		assert.Equal(t, tc.want, *mfs[0].Name)
	}
}
