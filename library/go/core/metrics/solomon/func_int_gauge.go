package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"
)

var _ Metric = (*FuncIntGauge)(nil)

// FuncIntGauge tracks int64 value returned by function.
type FuncIntGauge struct {
	name       string
	metricType metricType
	tags       map[string]string
	function   func() int64
	timestamp  *time.Time

	useNameTag bool
}

func (g *FuncIntGauge) Name() string {
	return g.name
}

func (g *FuncIntGauge) Function() func() int64 {
	return g.function
}

func (g *FuncIntGauge) getType() metricType {
	return g.metricType
}

func (g *FuncIntGauge) getLabels() map[string]string {
	return g.tags
}

func (g *FuncIntGauge) getValue() interface{} {
	return g.function()
}

func (g *FuncIntGauge) getTimestamp() *time.Time {
	return g.timestamp
}

func (g *FuncIntGauge) getNameTag() string {
	if g.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (g *FuncIntGauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     int64             `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type:  g.metricType.String(),
		Value: g.function(),
		Labels: func() map[string]string {
			labels := make(map[string]string, len(g.tags)+1)
			labels[g.getNameTag()] = g.Name()
			for k, v := range g.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(g.timestamp),
	})
}

// Snapshot returns independent copy on metric.
func (g *FuncIntGauge) Snapshot() Metric {
	return &IntGauge{
		name:       g.name,
		metricType: g.metricType,
		tags:       g.tags,
		value:      *atomic.NewInt64(g.function()),

		useNameTag: g.useNameTag,
	}
}
