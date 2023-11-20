package solomon

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestRegistry_Gather(t *testing.T) {
	r := &Registry{
		separator:     ".",
		prefix:        "myprefix",
		tags:          make(map[string]string),
		subregistries: make(map[string]*Registry),
		metrics: func() *sync.Map {
			metrics := map[string]Metric{
				"myprefix.mycounter": &Counter{
					name:  "myprefix.mycounter",
					tags:  map[string]string{"ololo": "trololo"},
					value: *atomic.NewInt64(42),
				},
				"myprefix.mygauge": &Gauge{
					name:  "myprefix.mygauge",
					tags:  map[string]string{"shimba": "boomba"},
					value: *atomic.NewFloat64(14.89),
				},
				"myprefix.mytimer": &Timer{
					name:  "myprefix.mytimer",
					tags:  map[string]string{"looken": "tooken"},
					value: *atomic.NewDuration(1456 * time.Millisecond),
				},
				"myprefix.myhistogram": &Histogram{
					name:         "myprefix.myhistogram",
					tags:         map[string]string{"chicken": "cooken"},
					bucketBounds: []float64{1, 2, 3},
					bucketValues: []int64{1, 2, 1},
					infValue:     *atomic.NewInt64(1),
				},
			}

			sm := new(sync.Map)
			for k, v := range metrics {
				sm.Store(k, v)
			}

			return sm
		}(),
	}

	s, err := r.Gather()
	assert.NoError(t, err)

	expected := &Metrics{}
	r.metrics.Range(func(_, s interface{}) bool {
		expected.metrics = append(expected.metrics, s.(Metric))
		return true
	})

	opts := cmp.Options{
		cmp.AllowUnexported(Metrics{}, Counter{}, Gauge{}, Timer{}, Histogram{}),
		cmpopts.IgnoreUnexported(sync.Mutex{}, atomic.Duration{}, atomic.Int64{}, atomic.Float64{}),
		// this will sort both slices for latest tests as well
		cmpopts.SortSlices(func(x, y Metric) bool {
			return x.Name() < y.Name()
		}),
	}

	assert.True(t, cmp.Equal(expected, s, opts...), cmp.Diff(expected, s, opts...))

	for _, sen := range s.metrics {
		var expectedMetric Metric
		for _, expSen := range expected.metrics {
			if expSen.Name() == sen.Name() {
				expectedMetric = expSen
				break
			}
		}
		require.NotNil(t, expectedMetric)

		assert.NotEqual(t, fmt.Sprintf("%p", expectedMetric), fmt.Sprintf("%p", sen))
		assert.IsType(t, expectedMetric, sen)

		switch st := sen.(type) {
		case *Counter:
			assert.NotEqual(t, fmt.Sprintf("%p", expectedMetric.(*Counter)), fmt.Sprintf("%p", st))
		case *Gauge:
			assert.NotEqual(t, fmt.Sprintf("%p", expectedMetric.(*Gauge)), fmt.Sprintf("%p", st))
		case *Timer:
			assert.NotEqual(t, fmt.Sprintf("%p", expectedMetric.(*Timer)), fmt.Sprintf("%p", st))
		case *Histogram:
			assert.NotEqual(t, fmt.Sprintf("%p", expectedMetric.(*Histogram)), fmt.Sprintf("%p", st))
		default:
			t.Fatalf("unexpected metric type: %T", sen)
		}
	}
}

func TestDoubleRegistration(t *testing.T) {
	r := NewRegistry(NewRegistryOpts())

	c0 := r.Counter("counter")
	c1 := r.Counter("counter")
	require.Equal(t, c0, c1)

	g0 := r.Gauge("counter")
	g1 := r.Gauge("counter")
	require.Equal(t, g0, g1)

	c2 := r.Counter("counter")
	require.NotEqual(t, reflect.ValueOf(c0).Elem().UnsafeAddr(), reflect.ValueOf(c2).Elem().UnsafeAddr())
}

func TestSubregistry(t *testing.T) {
	r := NewRegistry(NewRegistryOpts())

	r0 := r.WithPrefix("one")
	r1 := r0.WithPrefix("two")
	r2 := r0.WithTags(map[string]string{"foo": "bar"})

	_ = r0.Counter("counter")
	_ = r1.Counter("counter")
	_ = r2.Counter("counter")
}

func TestSubregistry_TagAndPrefixReorder(t *testing.T) {
	r := NewRegistry(NewRegistryOpts())

	r0 := r.WithPrefix("one")
	r1 := r.WithTags(map[string]string{"foo": "bar"})

	r3 := r0.WithTags(map[string]string{"foo": "bar"})
	r4 := r1.WithPrefix("one")

	require.True(t, r3 == r4)
}

func TestRatedRegistry(t *testing.T) {
	r := NewRegistry(NewRegistryOpts().SetRated(true))
	s := r.Counter("counter")
	b, _ := json.Marshal(s)
	expected := []byte(`{"type":"RATE","labels":{"sensor":"counter"},"value":0}`)
	assert.Equal(t, expected, b)
}

func TestNameTagRegistry(t *testing.T) {
	r := NewRegistry(NewRegistryOpts().SetUseNameTag(true))
	s := r.Counter("counter")

	b, _ := json.Marshal(s)
	expected := []byte(`{"type":"COUNTER","labels":{"name":"counter"},"value":0}`)
	assert.Equal(t, expected, b)

	sr := r.WithTags(map[string]string{"foo": "bar"})
	ssr := sr.Counter("sub_counter")

	b1, _ := json.Marshal(ssr)
	expected1 := []byte(`{"type":"COUNTER","labels":{"foo":"bar","name":"sub_counter"},"value":0}`)
	assert.Equal(t, expected1, b1)
}
