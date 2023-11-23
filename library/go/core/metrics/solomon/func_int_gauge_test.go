package solomon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestFuncIntGauge_Value(t *testing.T) {
	val := new(atomic.Int64)
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return val.Load()
		},
	}

	val.Store(1)
	assert.Equal(t, int64(1), c.Snapshot().(*IntGauge).value.Load())

	val.Store(42)
	assert.Equal(t, int64(42), c.Snapshot().(*IntGauge).value.Load())

}

func TestFunIntGauge_MarshalJSON(t *testing.T) {
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"IGAUGE","labels":{"ololo":"trololo","sensor":"myintgauge"},"value":42}`)
	assert.Equal(t, expected, b)
}

func TestNameTagFunIntGauge_MarshalJSON(t *testing.T) {
	c := &FuncIntGauge{
		name:       "myintgauge",
		metricType: typeIGauge,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"IGAUGE","labels":{"name":"myintgauge","ololo":"trololo"},"value":42}`)
	assert.Equal(t, expected, b)
}
