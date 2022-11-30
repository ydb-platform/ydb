package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"

	"a.yandex-team.ru/library/go/core/metrics"
)

var (
	_ metrics.Counter = (*Counter)(nil)
	_ Metric          = (*Counter)(nil)
)

// Counter tracks monotonically increasing value.
type Counter struct {
	name       string
	metricType metricType
	tags       map[string]string
	value      atomic.Int64
	timestamp  *time.Time

	useNameTag bool
}

// Inc increments counter by 1.
func (c *Counter) Inc() {
	c.Add(1)
}

// Add adds delta to the counter. Delta must be >=0.
func (c *Counter) Add(delta int64) {
	c.value.Add(delta)
}

func (c *Counter) Name() string {
	return c.name
}

func (c *Counter) getType() metricType {
	return c.metricType
}

func (c *Counter) getLabels() map[string]string {
	return c.tags
}

func (c *Counter) getValue() interface{} {
	return c.value.Load()
}

func (c *Counter) getTimestamp() *time.Time {
	return c.timestamp
}

func (c *Counter) getNameTag() string {
	if c.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     int64             `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type:  c.metricType.String(),
		Value: c.value.Load(),
		Labels: func() map[string]string {
			labels := make(map[string]string, len(c.tags)+1)
			labels[c.getNameTag()] = c.Name()
			for k, v := range c.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(c.timestamp),
	})
}

// Snapshot returns independent copy on metric.
func (c *Counter) Snapshot() Metric {
	return &Counter{
		name:       c.name,
		metricType: c.metricType,
		tags:       c.tags,
		value:      *atomic.NewInt64(c.value.Load()),

		useNameTag: c.useNameTag,
	}
}
