package prometheus

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestGauge_Add(t *testing.T) {
	g := &Gauge{gg: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_gauge_add",
	})}

	var expectValue float64 = 42
	g.Add(expectValue)

	var res dto.Metric
	err := g.gg.Write(&res)

	assert.NoError(t, err)
	assert.Equal(t, expectValue, res.GetGauge().GetValue())
}

func TestGauge_Set(t *testing.T) {
	g := &Gauge{gg: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_gauge_set",
	})}

	var expectValue float64 = 42
	g.Set(expectValue)

	var res dto.Metric
	err := g.gg.Write(&res)

	assert.NoError(t, err)
	assert.Equal(t, expectValue, res.GetGauge().GetValue())
}
