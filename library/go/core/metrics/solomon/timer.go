package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"

	"a.yandex-team.ru/library/go/core/metrics"
)

var (
	_ metrics.Timer = (*Timer)(nil)
	_ Metric        = (*Timer)(nil)
)

// Timer measures gauge duration.
type Timer struct {
	name       string
	metricType metricType
	tags       map[string]string
	value      atomic.Duration
	timestamp  *time.Time

	useNameTag bool
}

func (t *Timer) RecordDuration(value time.Duration) {
	t.value.Store(value)
}

func (t *Timer) Name() string {
	return t.name
}

func (t *Timer) getType() metricType {
	return t.metricType
}

func (t *Timer) getLabels() map[string]string {
	return t.tags
}

func (t *Timer) getValue() interface{} {
	return t.value.Load().Seconds()
}

func (t *Timer) getTimestamp() *time.Time {
	return t.timestamp
}

func (t *Timer) getNameTag() string {
	if t.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (t *Timer) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type:  t.metricType.String(),
		Value: t.value.Load().Seconds(),
		Labels: func() map[string]string {
			labels := make(map[string]string, len(t.tags)+1)
			labels[t.getNameTag()] = t.Name()
			for k, v := range t.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(t.timestamp),
	})
}

// Snapshot returns independent copy on metric.
func (t *Timer) Snapshot() Metric {
	return &Timer{
		name:       t.name,
		metricType: t.metricType,
		tags:       t.tags,
		value:      *atomic.NewDuration(t.value.Load()),

		useNameTag: t.useNameTag,
	}
}
