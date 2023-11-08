package prometheus

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestTimer_RecordDuration(t *testing.T) {
	g := &Timer{gg: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_timer_record_duration",
	})}

	g.RecordDuration(42 * time.Second)

	var res dto.Metric
	err := g.gg.Write(&res)

	assert.NoError(t, err)
	assert.Equal(t, float64(42), res.GetGauge().GetValue())
}
