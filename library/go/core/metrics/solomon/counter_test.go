package solomon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestCounter_Add(t *testing.T) {
	c := &Counter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
	}

	c.Add(1)
	assert.Equal(t, int64(1), c.value.Load())

	c.Add(42)
	assert.Equal(t, int64(43), c.value.Load())

	c.Add(1489)
	assert.Equal(t, int64(1532), c.value.Load())
}

func TestCounter_Inc(t *testing.T) {
	c := &Counter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
	}

	for i := 0; i < 10; i++ {
		c.Inc()
	}
	assert.Equal(t, int64(10), c.value.Load())

	c.Inc()
	c.Inc()
	assert.Equal(t, int64(12), c.value.Load())
}

func TestCounter_MarshalJSON(t *testing.T) {
	c := &Counter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewInt64(42),
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"COUNTER","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42}`)
	assert.Equal(t, expected, b)
}

func TestRatedCounter_MarshalJSON(t *testing.T) {
	c := &Counter{
		name:       "mycounter",
		metricType: typeRated,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewInt64(42),
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"RATE","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42}`)
	assert.Equal(t, expected, b)
}

func TestNameTagCounter_MarshalJSON(t *testing.T) {
	c := &Counter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewInt64(42),

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"COUNTER","labels":{"name":"mycounter","ololo":"trololo"},"value":42}`)
	assert.Equal(t, expected, b)
}
