package solomon

import "time"

type MetricsOpts struct {
	useNameTag bool
	tags       map[string]string
	timestamp  *time.Time
}

type MetricOpt func(*MetricsOpts)

func WithTags(tags map[string]string) func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.tags = tags
	}
}

func WithUseNameTag() func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.useNameTag = true
	}
}

func WithTimestamp(t time.Time) func(*MetricsOpts) {
	return func(m *MetricsOpts) {
		m.timestamp = &t
	}
}
