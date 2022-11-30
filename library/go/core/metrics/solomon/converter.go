package solomon

import (
	"fmt"

	dto "github.com/prometheus/client_model/go"
	"go.uber.org/atomic"
)

// PrometheusMetrics converts Prometheus metrics to Solomon metrics.
func PrometheusMetrics(metrics []*dto.MetricFamily) (*Metrics, error) {
	s := &Metrics{
		metrics: make([]Metric, 0, len(metrics)),
	}

	if len(metrics) == 0 {
		return s, nil
	}

	for _, mf := range metrics {
		if len(mf.Metric) == 0 {
			continue
		}

		metric := mf.Metric[0]

		tags := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			tags[label.GetName()] = label.GetValue()
		}

		switch *mf.Type {
		case dto.MetricType_COUNTER:
			s.metrics = append(s.metrics, &Counter{
				name:  mf.GetName(),
				tags:  tags,
				value: *atomic.NewInt64(int64(metric.Counter.GetValue())),
			})
		case dto.MetricType_GAUGE:
			s.metrics = append(s.metrics, &Gauge{
				name:  mf.GetName(),
				tags:  tags,
				value: *atomic.NewFloat64(metric.Gauge.GetValue()),
			})
		case dto.MetricType_HISTOGRAM:
			bounds := make([]float64, 0, len(metric.Histogram.Bucket))
			values := make([]int64, 0, len(metric.Histogram.Bucket))

			var prevValuesSum int64

			for _, bucket := range metric.Histogram.Bucket {
				// prometheus uses cumulative buckets where solomon uses instant
				bucketValue := int64(bucket.GetCumulativeCount())
				bucketValue -= prevValuesSum
				prevValuesSum += bucketValue

				bounds = append(bounds, bucket.GetUpperBound())
				values = append(values, bucketValue)
			}

			s.metrics = append(s.metrics, &Histogram{
				name:         mf.GetName(),
				tags:         tags,
				bucketBounds: bounds,
				bucketValues: values,
			})
		default:
			return nil, fmt.Errorf("unsupported type: %s", mf.Type.String())
		}
	}

	return s, nil
}
