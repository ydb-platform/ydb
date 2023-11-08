package solomon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

func TestMetrics_MarshalJSON(t *testing.T) {
	s := &Metrics{
		metrics: []Metric{
			&Counter{
				name:       "mycounter",
				metricType: typeCounter,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
			&Counter{
				name:       "myratedcounter",
				metricType: typeRated,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
			&Gauge{
				name:       "mygauge",
				metricType: typeGauge,
				tags:       map[string]string{"shimba": "boomba"},
				value:      *atomic.NewFloat64(14.89),
			},
			&Timer{
				name:       "mytimer",
				metricType: typeGauge,
				tags:       map[string]string{"looken": "tooken"},
				value:      *atomic.NewDuration(1456 * time.Millisecond),
			},
			&Histogram{
				name:         "myhistogram",
				metricType:   typeHistogram,
				tags:         map[string]string{"chicken": "cooken"},
				bucketBounds: []float64{1, 2, 3},
				bucketValues: []int64{1, 2, 1},
				infValue:     *atomic.NewInt64(1),
			},
			&Histogram{
				name:         "myratedhistogram",
				metricType:   typeRatedHistogram,
				tags:         map[string]string{"chicken": "cooken"},
				bucketBounds: []float64{1, 2, 3},
				bucketValues: []int64{1, 2, 1},
				infValue:     *atomic.NewInt64(1),
			},
			&Gauge{
				name:       "mytimedgauge",
				metricType: typeGauge,
				tags:       map[string]string{"oki": "toki"},
				value:      *atomic.NewFloat64(42.24),
				timestamp:  timeAsRef(time.Unix(1500000000, 0)),
			},
		},
	}

	b, err := json.Marshal(s)
	assert.NoError(t, err)

	expected := []byte(`{"metrics":[` +
		`{"type":"COUNTER","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42},` +
		`{"type":"RATE","labels":{"ololo":"trololo","sensor":"myratedcounter"},"value":42},` +
		`{"type":"DGAUGE","labels":{"sensor":"mygauge","shimba":"boomba"},"value":14.89},` +
		`{"type":"DGAUGE","labels":{"looken":"tooken","sensor":"mytimer"},"value":1.456},` +
		`{"type":"HIST","labels":{"chicken":"cooken","sensor":"myhistogram"},"hist":{"bounds":[1,2,3],"buckets":[1,2,1],"inf":1}},` +
		`{"type":"HIST_RATE","labels":{"chicken":"cooken","sensor":"myratedhistogram"},"hist":{"bounds":[1,2,3],"buckets":[1,2,1],"inf":1}},` +
		`{"type":"DGAUGE","labels":{"oki":"toki","sensor":"mytimedgauge"},"value":42.24,"ts":1500000000}` +
		`]}`)
	assert.Equal(t, expected, b)
}

func timeAsRef(t time.Time) *time.Time {
	return &t
}

func TestMetrics_with_timestamp_MarshalJSON(t *testing.T) {
	s := &Metrics{
		metrics: []Metric{
			&Counter{
				name:       "mycounter",
				metricType: typeCounter,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
			&Gauge{
				name:       "mytimedgauge",
				metricType: typeGauge,
				tags:       map[string]string{"oki": "toki"},
				value:      *atomic.NewFloat64(42.24),
				timestamp:  timeAsRef(time.Unix(1500000000, 0)),
			},
		},
		timestamp: timeAsRef(time.Unix(1657710477, 0)),
	}

	b, err := json.Marshal(s)
	assert.NoError(t, err)

	expected := []byte(`{"metrics":[` +
		`{"type":"COUNTER","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42},` +
		`{"type":"DGAUGE","labels":{"oki":"toki","sensor":"mytimedgauge"},"value":42.24,"ts":1500000000}` +
		`],"ts":1657710477}`)
	assert.Equal(t, expected, b)
}

func TestRated(t *testing.T) {
	testCases := []struct {
		name     string
		s        interface{}
		expected Metric
	}{
		{
			"counter",
			&Counter{
				name:       "mycounter",
				metricType: typeCounter,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
			&Counter{
				name:       "mycounter",
				metricType: typeRated,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
		},
		{
			"gauge",
			&Gauge{
				name:       "mygauge",
				metricType: typeGauge,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewFloat64(42),
			},
			&Gauge{
				name:       "mygauge",
				metricType: typeGauge,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewFloat64(42),
			},
		},
		{
			"timer",
			&Timer{
				name:       "mytimer",
				metricType: typeGauge,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewDuration(1 * time.Second),
			},
			&Timer{
				name:       "mytimer",
				metricType: typeGauge,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewDuration(1 * time.Second),
			},
		},
		{
			"histogram",
			&Histogram{
				name:         "myhistogram",
				metricType:   typeHistogram,
				tags:         map[string]string{"ololo": "trololo"},
				bucketBounds: []float64{1, 2, 3},
				infValue:     *atomic.NewInt64(0),
			},
			&Histogram{
				name:         "myhistogram",
				metricType:   typeRatedHistogram,
				tags:         map[string]string{"ololo": "trololo"},
				bucketBounds: []float64{1, 2, 3},
				infValue:     *atomic.NewInt64(0),
			},
		},
		{
			"metric_interface",
			metrics.Counter(&Counter{
				name:       "mycounter",
				metricType: typeCounter,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			}),
			&Counter{
				name:       "mycounter",
				metricType: typeRated,
				tags:       map[string]string{"ololo": "trololo"},
				value:      *atomic.NewInt64(42),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			Rated(tc.s)
			assert.Equal(t, tc.expected, tc.s)
		})
	}
}

func TestSplitToChunks(t *testing.T) {
	zeroMetrics := Metrics{
		metrics: []Metric{},
	}
	oneMetric := Metrics{
		metrics: []Metric{
			&Counter{name: "a"},
		},
	}
	twoMetrics := Metrics{
		metrics: []Metric{
			&Counter{name: "a"},
			&Counter{name: "b"},
		},
	}
	fourMetrics := Metrics{
		metrics: []Metric{
			&Counter{name: "a"},
			&Counter{name: "b"},
			&Counter{name: "c"},
			&Counter{name: "d"},
		},
	}
	fiveMetrics := Metrics{
		metrics: []Metric{
			&Counter{name: "a"},
			&Counter{name: "b"},
			&Counter{name: "c"},
			&Counter{name: "d"},
			&Counter{name: "e"},
		},
	}

	chunks := zeroMetrics.SplitToChunks(2)
	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, 0, len(chunks[0].metrics))

	chunks = oneMetric.SplitToChunks(1)
	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, 1, len(chunks[0].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())

	chunks = oneMetric.SplitToChunks(2)
	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, 1, len(chunks[0].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())

	chunks = twoMetrics.SplitToChunks(1)
	assert.Equal(t, 2, len(chunks))
	assert.Equal(t, 1, len(chunks[0].metrics))
	assert.Equal(t, 1, len(chunks[1].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())
	assert.Equal(t, "b", chunks[1].metrics[0].Name())

	chunks = twoMetrics.SplitToChunks(2)
	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, 2, len(chunks[0].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())
	assert.Equal(t, "b", chunks[0].metrics[1].Name())

	chunks = fourMetrics.SplitToChunks(2)
	assert.Equal(t, 2, len(chunks))
	assert.Equal(t, 2, len(chunks[0].metrics))
	assert.Equal(t, 2, len(chunks[1].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())
	assert.Equal(t, "b", chunks[0].metrics[1].Name())
	assert.Equal(t, "c", chunks[1].metrics[0].Name())
	assert.Equal(t, "d", chunks[1].metrics[1].Name())

	chunks = fiveMetrics.SplitToChunks(2)
	assert.Equal(t, 3, len(chunks))
	assert.Equal(t, 2, len(chunks[0].metrics))
	assert.Equal(t, 2, len(chunks[1].metrics))
	assert.Equal(t, 1, len(chunks[2].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())
	assert.Equal(t, "b", chunks[0].metrics[1].Name())
	assert.Equal(t, "c", chunks[1].metrics[0].Name())
	assert.Equal(t, "d", chunks[1].metrics[1].Name())
	assert.Equal(t, "e", chunks[2].metrics[0].Name())

	chunks = fiveMetrics.SplitToChunks(0)
	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, 5, len(chunks[0].metrics))
	assert.Equal(t, "a", chunks[0].metrics[0].Name())
	assert.Equal(t, "b", chunks[0].metrics[1].Name())
	assert.Equal(t, "c", chunks[0].metrics[2].Name())
	assert.Equal(t, "d", chunks[0].metrics[3].Name())
	assert.Equal(t, "e", chunks[0].metrics[4].Name())
}
