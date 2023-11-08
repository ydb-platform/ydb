package solomon

import (
	"encoding/json"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var (
	_ metrics.IntGauge = (*IntGauge)(nil)
	_ Metric           = (*IntGauge)(nil)
)

// IntGauge tracks single float64 value.
type IntGauge struct {
	name       string
	metricType metricType
	tags       map[string]string
	value      atomic.Int64
	timestamp  *time.Time

	useNameTag bool
}

func NewIntGauge(name string, value int64, opts ...metricOpts) IntGauge {
	mOpts := MetricsOpts{}
	for _, op := range opts {
		op(&mOpts)
	}
	return IntGauge{
		name:       name,
		metricType: typeIGauge,
		tags:       mOpts.tags,
		value:      *atomic.NewInt64(value),
		useNameTag: mOpts.useNameTag,
		timestamp:  mOpts.timestamp,
	}
}

func (g *IntGauge) Set(value int64) {
	g.value.Store(value)
}

func (g *IntGauge) Add(value int64) {
	g.value.Add(value)
}

func (g *IntGauge) Name() string {
	return g.name
}

func (g *IntGauge) getType() metricType {
	return g.metricType
}

func (g *IntGauge) getLabels() map[string]string {
	return g.tags
}

func (g *IntGauge) getValue() interface{} {
	return g.value.Load()
}

func (g *IntGauge) getTimestamp() *time.Time {
	return g.timestamp
}

func (g *IntGauge) getNameTag() string {
	if g.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (g *IntGauge) MarshalJSON() ([]byte, error) {
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
		Value     int64             `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type:      metricType,
		Value:     value,
		Labels:    labels,
		Timestamp: tsAsRef(g.timestamp),
	})
}

// Snapshot returns independent copy of metric.
func (g *IntGauge) Snapshot() Metric {
	return &IntGauge{
		name:       g.name,
		metricType: g.metricType,
		tags:       g.tags,
		value:      *atomic.NewInt64(g.value.Load()),

		useNameTag: g.useNameTag,
		timestamp:  g.timestamp,
	}
}
