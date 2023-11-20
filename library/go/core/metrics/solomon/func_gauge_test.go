package solomon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestFuncGauge_Value(t *testing.T) {
	val := new(atomic.Float64)
	c := &FuncGauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() float64 {
			return val.Load()
		},
	}

	val.Store(1)
	assert.Equal(t, float64(1), c.Snapshot().(*Gauge).value.Load())

	val.Store(42)
	assert.Equal(t, float64(42), c.Snapshot().(*Gauge).value.Load())

}

func TestFunGauge_MarshalJSON(t *testing.T) {
	c := &FuncGauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() float64 {
			return 42.18
		},
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"DGAUGE","labels":{"ololo":"trololo","sensor":"mygauge"},"value":42.18}`)
	assert.Equal(t, expected, b)
}

func TestNameTagFunGauge_MarshalJSON(t *testing.T) {
	c := &FuncGauge{
		name:       "mygauge",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() float64 {
			return 42.18
		},

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"DGAUGE","labels":{"name":"mygauge","ololo":"trololo"},"value":42.18}`)
	assert.Equal(t, expected, b)
}
