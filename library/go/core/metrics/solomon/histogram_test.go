package solomon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestHistogram_MarshalJSON(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: []int64{1, 2, 1},
		infValue:     *atomic.NewInt64(2),
	}

	b, err := json.Marshal(h)
	assert.NoError(t, err)

	expected := []byte(`{"type":"HIST","labels":{"ololo":"trololo","sensor":"myhistogram"},"hist":{"bounds":[1,2,3],"buckets":[1,2,1],"inf":2}}`)
	assert.Equal(t, expected, b)
}

func TestRatedHistogram_MarshalJSON(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeRatedHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: []int64{1, 2, 1},
		infValue:     *atomic.NewInt64(2),
	}

	b, err := json.Marshal(h)
	assert.NoError(t, err)

	expected := []byte(`{"type":"HIST_RATE","labels":{"ololo":"trololo","sensor":"myhistogram"},"hist":{"bounds":[1,2,3],"buckets":[1,2,1],"inf":2}}`)
	assert.Equal(t, expected, b)
}

func TestNameTagHistogram_MarshalJSON(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeRatedHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: []int64{1, 2, 1},
		infValue:     *atomic.NewInt64(2),
		useNameTag:   true,
	}

	b, err := json.Marshal(h)
	assert.NoError(t, err)

	expected := []byte(`{"type":"HIST_RATE","labels":{"name":"myhistogram","ololo":"trololo"},"hist":{"bounds":[1,2,3],"buckets":[1,2,1],"inf":2}}`)
	assert.Equal(t, expected, b)
}

func TestHistogram_RecordDuration(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: make([]int64, 3),
	}

	h.RecordDuration(500 * time.Millisecond)
	h.RecordDuration(1 * time.Second)
	h.RecordDuration(1800 * time.Millisecond)
	h.RecordDuration(3 * time.Second)
	h.RecordDuration(1 * time.Hour)

	expectedValues := []int64{2, 1, 1}
	assert.Equal(t, expectedValues, h.bucketValues)

	var expectedInfValue int64 = 1
	assert.Equal(t, expectedInfValue, h.infValue.Load())
}

func TestHistogram_RecordValue(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: make([]int64, 3),
	}

	h.RecordValue(0.5)
	h.RecordValue(1)
	h.RecordValue(1.8)
	h.RecordValue(3)
	h.RecordValue(60)

	expectedValues := []int64{2, 1, 1}
	assert.Equal(t, expectedValues, h.bucketValues)

	var expectedInfValue int64 = 1
	assert.Equal(t, expectedInfValue, h.infValue.Load())
}

func TestHistogram_Reset(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: make([]int64, 3),
	}

	h.RecordValue(0.5)
	h.RecordValue(1)
	h.RecordValue(1.8)
	h.RecordValue(3)
	h.RecordValue(60)

	assert.Equal(t, []int64{2, 1, 1}, h.bucketValues)
	assert.Equal(t, int64(1), h.infValue.Load())

	h.Reset()

	assert.Equal(t, []int64{0, 0, 0}, h.bucketValues)
	assert.Equal(t, int64(0), h.infValue.Load())
}

func TestHistogram_InitBucketValues(t *testing.T) {
	h := &Histogram{
		name:         "myhistogram",
		metricType:   typeHistogram,
		tags:         map[string]string{"ololo": "trololo"},
		bucketBounds: []float64{1, 2, 3},
		bucketValues: make([]int64, 3),
	}

	valsToInit := []int64{1, 2, 3, 4}
	h.InitBucketValues(valsToInit[:2])
	assert.Equal(t, append(valsToInit[:2], 0), h.bucketValues)
	assert.Equal(t, *atomic.NewInt64(0), h.infValue)

	h.InitBucketValues(valsToInit[:3])
	assert.Equal(t, valsToInit[:3], h.bucketValues)
	assert.Equal(t, *atomic.NewInt64(0), h.infValue)

	h.InitBucketValues(valsToInit)
	assert.Equal(t, valsToInit[:3], h.bucketValues)
	assert.Equal(t, *atomic.NewInt64(valsToInit[3]), h.infValue)
}
