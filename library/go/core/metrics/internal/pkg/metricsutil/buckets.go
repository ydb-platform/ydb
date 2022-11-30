package metricsutil

import (
	"sort"

	"a.yandex-team.ru/library/go/core/metrics"
)

// BucketsBounds unwraps Buckets bounds to slice of float64.
func BucketsBounds(b metrics.Buckets) []float64 {
	bkts := make([]float64, b.Size())
	for i := range bkts {
		bkts[i] = b.UpperBound(i)
	}
	sort.Float64s(bkts)
	return bkts
}

// DurationBucketsBounds unwraps DurationBuckets bounds to slice of float64.
func DurationBucketsBounds(b metrics.DurationBuckets) []float64 {
	bkts := make([]float64, b.Size())
	for i := range bkts {
		bkts[i] = b.UpperBound(i).Seconds()
	}
	sort.Float64s(bkts)
	return bkts
}
