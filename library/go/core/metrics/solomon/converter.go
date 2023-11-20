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

		for _, metric := range mf.Metric {

			tags := make(map[string]string, len(metric.Label))
			for _, label := range metric.Label {
				tags[label.GetName()] = label.GetValue()
			}

			switch *mf.Type {
			case dto.MetricType_COUNTER:
				s.metrics = append(s.metrics, &Counter{
					name:       mf.GetName(),
					metricType: typeCounter,
					tags:       tags,
					value:      *atomic.NewInt64(int64(metric.Counter.GetValue())),
				})
			case dto.MetricType_GAUGE:
				s.metrics = append(s.metrics, &Gauge{
					name:       mf.GetName(),
					metricType: typeGauge,
					tags:       tags,
					value:      *atomic.NewFloat64(metric.Gauge.GetValue()),
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
					metricType:   typeHistogram,
					tags:         tags,
					bucketBounds: bounds,
					bucketValues: values,
					infValue:     *atomic.NewInt64(int64(metric.Histogram.GetSampleCount()) - prevValuesSum),
				})
			case dto.MetricType_SUMMARY:
				bounds := make([]float64, 0, len(metric.Summary.Quantile))
				values := make([]int64, 0, len(metric.Summary.Quantile))

				var prevValuesSum int64

				for _, bucket := range metric.Summary.GetQuantile() {
					// prometheus uses cumulative buckets where solomon uses instant
					bucketValue := int64(bucket.GetValue())
					bucketValue -= prevValuesSum
					prevValuesSum += bucketValue

					bounds = append(bounds, bucket.GetQuantile())
					values = append(values, bucketValue)
				}

				mName := mf.GetName()

				s.metrics = append(s.metrics, &Histogram{
					name:         mName,
					metricType:   typeHistogram,
					tags:         tags,
					bucketBounds: bounds,
					bucketValues: values,
					infValue:     *atomic.NewInt64(int64(*metric.Summary.SampleCount) - prevValuesSum),
				}, &Counter{
					name:       mName + "_count",
					metricType: typeCounter,
					tags:       tags,
					value:      *atomic.NewInt64(int64(*metric.Summary.SampleCount)),
				}, &Gauge{
					name:       mName + "_sum",
					metricType: typeGauge,
					tags:       tags,
					value:      *atomic.NewFloat64(*metric.Summary.SampleSum),
				})
			default:
				return nil, fmt.Errorf("unsupported type: %s", mf.Type.String())
			}
		}
	}

	return s, nil
}
