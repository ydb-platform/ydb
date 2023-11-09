package prometheus

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestCounter_Add(t *testing.T) {
	c := &Counter{cnt: prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_counter_add",
	})}

	var expectValue int64 = 42
	c.Add(expectValue)

	var res dto.Metric
	err := c.cnt.Write(&res)

	assert.NoError(t, err)
	assert.Equal(t, expectValue, int64(res.GetCounter().GetValue()))
}

func TestCounter_Inc(t *testing.T) {
	c := &Counter{cnt: prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_counter_inc",
	})}

	var res dto.Metric
	for i := 1; i <= 10; i++ {
		c.Inc()
		err := c.cnt.Write(&res)
		assert.NoError(t, err)
		assert.Equal(t, int64(i), int64(res.GetCounter().GetValue()))
	}
}
