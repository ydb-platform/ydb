package solomon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestGauge_Add(t *testing.T) {
	c := &Gauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
	}

	c.Add(1)
	assert.Equal(t, float64(1), c.value.Load())

	c.Add(42)
	assert.Equal(t, float64(43), c.value.Load())

	c.Add(14.89)
	assert.Equal(t, float64(57.89), c.value.Load())
}

func TestGauge_Set(t *testing.T) {
	c := &Gauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
	}

	c.Set(1)
	assert.Equal(t, float64(1), c.value.Load())

	c.Set(42)
	assert.Equal(t, float64(42), c.value.Load())

	c.Set(14.89)
	assert.Equal(t, float64(14.89), c.value.Load())
}

func TestGauge_MarshalJSON(t *testing.T) {
	c := &Gauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewFloat64(42.18),
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"DGAUGE","labels":{"ololo":"trololo","sensor":"mygauge"},"value":42.18}`)
	assert.Equal(t, expected, b)
}

func TestNameTagGauge_MarshalJSON(t *testing.T) {
	c := &Gauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewFloat64(42.18),

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"DGAUGE","labels":{"name":"mygauge","ololo":"trololo"},"value":42.18}`)
	assert.Equal(t, expected, b)
}
