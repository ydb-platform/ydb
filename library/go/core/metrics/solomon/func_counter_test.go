package solomon

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestFuncCounter_Inc(t *testing.T) {
	val := new(atomic.Int64)
	c := &FuncCounter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return val.Load()
		},
	}

	val.Store(1)
	assert.Equal(t, int64(1), c.Snapshot().(*Counter).value.Load())

	val.Store(42)
	assert.Equal(t, int64(42), c.Snapshot().(*Counter).value.Load())

}

func TestFuncCounter_MarshalJSON(t *testing.T) {
	c := &FuncCounter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"COUNTER","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42}`)
	assert.Equal(t, expected, b)
}

func TestRatedFuncCounter_MarshalJSON(t *testing.T) {
	c := &FuncCounter{
		name:       "mycounter",
		metricType: typeRated,
		tags:       map[string]string{"ololo": "trololo"},
		function: func() int64 {
			return 42
		},
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"RATE","labels":{"ololo":"trololo","sensor":"mycounter"},"value":42}`)
	assert.Equal(t, expected, b)
}

func TestNameTagFuncCounter_MarshalJSON(t *testing.T) {
	c := &FuncCounter{
		name:       "mycounter",
		metricType: typeCounter,
		tags:       map[string]string{"ololo": "trololo"},

		function: func() int64 {
			return 42
		},

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"COUNTER","labels":{"name":"mycounter","ololo":"trololo"},"value":42}`)
	assert.Equal(t, expected, b)
}
