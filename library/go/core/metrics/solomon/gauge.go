package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"

	"a.yandex-team.ru/library/go/core/metrics"
)

var (
	_ metrics.Gauge = (*Gauge)(nil)
	_ Metric        = (*Gauge)(nil)
)

// Gauge tracks single float64 value.
type Gauge struct {
	name       string
	metricType metricType
	tags       map[string]string
	value      atomic.Float64
	timestamp  *time.Time

	useNameTag bool
}

func NewGauge(name string, value float64, opts ...metricOpts) Gauge {
	mOpts := MetricsOpts{}
	for _, op := range opts {
		op(&mOpts)
	}
	return Gauge{
		name:       name,
		metricType: typeGauge,
		tags:       mOpts.tags,
		value:      *atomic.NewFloat64(value),
		useNameTag: mOpts.useNameTag,
		timestamp:  mOpts.timestamp,
	}
}

func (g *Gauge) Set(value float64) {
	g.value.Store(value)
}

func (g *Gauge) Add(value float64) {
	g.value.Add(value)
}

func (g *Gauge) Name() string {
	return g.name
}

func (g *Gauge) getType() metricType {
	return g.metricType
}

func (g *Gauge) getLabels() map[string]string {
	return g.tags
}

func (g *Gauge) getValue() interface{} {
	return g.value.Load()
}

func (g *Gauge) getTimestamp() *time.Time {
	return g.timestamp
}

func (g *Gauge) getNameTag() string {
	if g.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	metricType := g.metricType.String()
	value := g.value.Load()
	labels := func() map[string]string {
		labels := make(map[string]string, len(g.tags)+1)
		labels[g.getNameTag()] = g.Name()
		for k, v := range g.tags {
			labels[k] = v
		}
		return labels
	}()

	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type:      metricType,
		Value:     value,
		Labels:    labels,
		Timestamp: tsAsRef(g.timestamp),
	})
}

// Snapshot returns independent copy of metric.
func (g *Gauge) Snapshot() Metric {
	return &Gauge{
		name:       g.name,
		metricType: g.metricType,
		tags:       g.tags,
		value:      *atomic.NewFloat64(g.value.Load()),

		useNameTag: g.useNameTag,
		timestamp:  g.timestamp,
	}
}
