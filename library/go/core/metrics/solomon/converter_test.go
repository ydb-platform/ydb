package solomon

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/ptr"
	"go.uber.org/atomic"
)

func TestPrometheusMetrics(t *testing.T) {
	testCases := []struct {
		name      string
		metrics   []*dto.MetricFamily
		expect    *Metrics
		expectErr error
	}{
		{
			name: "success",
			metrics: []*dto.MetricFamily{
				{
					Name: ptr.String("subregister1_mygauge"),
					Help: ptr.String(""),
					Type: ptr.T(dto.MetricType_GAUGE),
					Metric: []*dto.Metric{
						{
							Label: []*dto.LabelPair{
								{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
							},
							Gauge: &dto.Gauge{Value: ptr.Float64(42)},
						},
					},
				},
				{
					Name: ptr.String("subregisters_count"),
					Help: ptr.String(""),
					Type: ptr.T(dto.MetricType_COUNTER),
					Metric: []*dto.Metric{
						{
							Label:   []*dto.LabelPair{},
							Counter: &dto.Counter{Value: ptr.Float64(2)},
						},
					},
				},
				{
					Name: ptr.String("subregister1_subregister2_myhistogram"),
					Help: ptr.String(""),
					Type: ptr.T(dto.MetricType_HISTOGRAM),
					Metric: []*dto.Metric{
						{
							Label: []*dto.LabelPair{
								{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
								{Name: ptr.String("shimba"), Value: ptr.String("boomba")},
							},
							Histogram: &dto.Histogram{
								SampleCount: ptr.Uint64(6),
								SampleSum:   ptr.Float64(4.2),
								Bucket: []*dto.Bucket{
									{CumulativeCount: ptr.Uint64(1), UpperBound: ptr.Float64(1)}, // 0.5 written
									{CumulativeCount: ptr.Uint64(3), UpperBound: ptr.Float64(2)}, // 1.5 & 1.7 written
									{CumulativeCount: ptr.Uint64(4), UpperBound: ptr.Float64(3)}, // 2.2 written
								},
							},
						},
					},
				},
				{
					Name: ptr.String("metrics_group"),
					Help: ptr.String(""),
					Type: ptr.T(dto.MetricType_COUNTER),
					Metric: []*dto.Metric{
						{
							Label:   []*dto.LabelPair{},
							Counter: &dto.Counter{Value: ptr.Float64(2)},
						},
						{
							Label:   []*dto.LabelPair{},
							Counter: &dto.Counter{Value: ptr.Float64(3)},
						},
					},
				},
			},
			expect: &Metrics{
				metrics: []Metric{
					&Gauge{
						name:       "subregister1_mygauge",
						metricType: typeGauge,
						tags:       map[string]string{"ololo": "trololo"},
						value:      *atomic.NewFloat64(42),
					},
					&Counter{
						name:       "subregisters_count",
						metricType: typeCounter,
						tags:       map[string]string{},
						value:      *atomic.NewInt64(2),
					},
					&Histogram{
						name:         "subregister1_subregister2_myhistogram",
						metricType:   typeHistogram,
						tags:         map[string]string{"ololo": "trololo", "shimba": "boomba"},
						bucketBounds: []float64{1, 2, 3},
						bucketValues: []int64{1, 2, 1},
						infValue:     *atomic.NewInt64(2),
					},
					// group of metrics
					&Counter{
						name:       "metrics_group",
						metricType: typeCounter,
						tags:       map[string]string{},
						value:      *atomic.NewInt64(2),
					},
					&Counter{
						name:       "metrics_group",
						metricType: typeCounter,
						tags:       map[string]string{},
						value:      *atomic.NewInt64(3),
					},
				},
			},
			expectErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := PrometheusMetrics(tc.metrics)

			if tc.expectErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectErr.Error())
			}

			assert.Equal(t, tc.expect, s)
		})
	}
}

func TestPrometheusSummaryMetric(t *testing.T) {
	src := []*dto.MetricFamily{
		{
			Name: ptr.String("subregister1_subregister2_mysummary"),
			Help: ptr.String(""),
			Type: func(mt dto.MetricType) *dto.MetricType { return &mt }(dto.MetricType_SUMMARY),
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{Name: ptr.String("ololo"), Value: ptr.String("trololo")},
						{Name: ptr.String("shimba"), Value: ptr.String("boomba")},
					},
					Summary: &dto.Summary{
						SampleCount: ptr.Uint64(8),
						SampleSum:   ptr.Float64(4.2),
						Quantile: []*dto.Quantile{
							{Value: ptr.Float64(1), Quantile: ptr.Float64(1)}, // 0.5 written
							{Value: ptr.Float64(3), Quantile: ptr.Float64(2)}, // 1.5 & 1.7 written
							{Value: ptr.Float64(4), Quantile: ptr.Float64(3)}, // 2.2 written
						},
					},
				},
			},
		},
	}

	mName := "subregister1_subregister2_mysummary"
	mTags := map[string]string{"ololo": "trololo", "shimba": "boomba"}
	bBounds := []float64{1, 2, 3}
	bValues := []int64{1, 2, 1}

	expect := &Metrics{
		metrics: []Metric{
			&Histogram{
				name:         mName,
				metricType:   typeHistogram,
				tags:         mTags,
				bucketBounds: bBounds,
				bucketValues: bValues,
				infValue:     *atomic.NewInt64(4),
			},
			&Counter{
				name:       mName + "_count",
				metricType: typeCounter,
				tags:       mTags,
				value:      *atomic.NewInt64(8),
			},
			&Gauge{
				name:       mName + "_sum",
				metricType: typeGauge,
				tags:       mTags,
				value:      *atomic.NewFloat64(4.2),
			},
		},
	}

	s, err := PrometheusMetrics(src)
	assert.NoError(t, err)

	assert.Equal(t, expect, s)
}
