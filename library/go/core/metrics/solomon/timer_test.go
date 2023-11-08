package solomon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestTimer_RecordDuration(t *testing.T) {
	c := &Timer{
		name:       "mytimer",
		metricType: typeGauge,
		tags:       map[string]string{"ololo": "trololo"},
	}

	c.RecordDuration(1 * time.Second)
	assert.Equal(t, 1*time.Second, c.value.Load())

	c.RecordDuration(42 * time.Millisecond)
	assert.Equal(t, 42*time.Millisecond, c.value.Load())
}

func TestTimerRated_MarshalJSON(t *testing.T) {
	c := &Timer{
		name:       "mytimer",
		metricType: typeRated,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewDuration(42 * time.Millisecond),
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"RATE","labels":{"ololo":"trololo","sensor":"mytimer"},"value":0.042}`)
	assert.Equal(t, expected, b)
}

func TestNameTagTimer_MarshalJSON(t *testing.T) {
	c := &Timer{
		name:       "mytimer",
		metricType: typeRated,
		tags:       map[string]string{"ololo": "trololo"},
		value:      *atomic.NewDuration(42 * time.Millisecond),

		useNameTag: true,
	}

	b, err := json.Marshal(c)
	assert.NoError(t, err)

	expected := []byte(`{"type":"RATE","labels":{"name":"mytimer","ololo":"trololo"},"value":0.042}`)
	assert.Equal(t, expected, b)
}
