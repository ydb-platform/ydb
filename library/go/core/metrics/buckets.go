package metrics

import (
	"sort"
	"time"
)

var (
	_ DurationBuckets = (*durationBuckets)(nil)
	_ Buckets         = (*buckets)(nil)
)

const (
	errBucketsCountNeedsGreaterThanZero = "n needs to be > 0"
	errBucketsStartNeedsGreaterThanZero = "start needs to be > 0"
	errBucketsFactorNeedsGreaterThanOne = "factor needs to be > 1"
)

type durationBuckets struct {
	buckets []time.Duration
}

// NewDurationBuckets returns new DurationBuckets implementation.
func NewDurationBuckets(bk ...time.Duration) DurationBuckets {
	sort.Slice(bk, func(i, j int) bool {
		return bk[i] < bk[j]
	})
	return durationBuckets{buckets: bk}
}

func (d durationBuckets) Size() int {
	return len(d.buckets)
}

func (d durationBuckets) MapDuration(dv time.Duration) (idx int) {
	for _, bound := range d.buckets {
		if dv < bound {
			break
		}
		idx++
	}
	return
}

func (d durationBuckets) UpperBound(idx int) time.Duration {
	if idx > d.Size()-1 {
		panic("idx is out of bounds")
	}
	return d.buckets[idx]
}

type buckets struct {
	buckets []float64
}

// NewBuckets returns new Buckets implementation.
func NewBuckets(bk ...float64) Buckets {
	sort.Slice(bk, func(i, j int) bool {
		return bk[i] < bk[j]
	})
	return buckets{buckets: bk}
}

func (d buckets) Size() int {
	return len(d.buckets)
}

func (d buckets) MapValue(v float64) (idx int) {
	for _, bound := range d.buckets {
		if v < bound {
			break
		}
		idx++
	}
	return
}

func (d buckets) UpperBound(idx int) float64 {
	if idx > d.Size()-1 {
		panic("idx is out of bounds")
	}
	return d.buckets[idx]
}

// MakeLinearBuckets creates a set of linear value buckets.
func MakeLinearBuckets(start, width float64, n int) Buckets {
	if n <= 0 {
		panic(errBucketsCountNeedsGreaterThanZero)
	}
	bounds := make([]float64, n)
	for i := range bounds {
		bounds[i] = start + (float64(i) * width)
	}
	return NewBuckets(bounds...)
}

// MakeLinearDurationBuckets creates a set of linear duration buckets.
func MakeLinearDurationBuckets(start, width time.Duration, n int) DurationBuckets {
	if n <= 0 {
		panic(errBucketsCountNeedsGreaterThanZero)
	}
	buckets := make([]time.Duration, n)
	for i := range buckets {
		buckets[i] = start + (time.Duration(i) * width)
	}
	return NewDurationBuckets(buckets...)
}

// MakeExponentialBuckets creates a set of exponential value buckets.
func MakeExponentialBuckets(start, factor float64, n int) Buckets {
	if n <= 0 {
		panic(errBucketsCountNeedsGreaterThanZero)
	}
	if start <= 0 {
		panic(errBucketsStartNeedsGreaterThanZero)
	}
	if factor <= 1 {
		panic(errBucketsFactorNeedsGreaterThanOne)
	}
	buckets := make([]float64, n)
	curr := start
	for i := range buckets {
		buckets[i] = curr
		curr *= factor
	}
	return NewBuckets(buckets...)
}

// MakeExponentialDurationBuckets creates a set of exponential duration buckets.
func MakeExponentialDurationBuckets(start time.Duration, factor float64, n int) DurationBuckets {
	if n <= 0 {
		panic(errBucketsCountNeedsGreaterThanZero)
	}
	if start <= 0 {
		panic(errBucketsStartNeedsGreaterThanZero)
	}
	if factor <= 1 {
		panic(errBucketsFactorNeedsGreaterThanOne)
	}
	buckets := make([]time.Duration, n)
	curr := start
	for i := range buckets {
		buckets[i] = curr
		curr = time.Duration(float64(curr) * factor)
	}
	return NewDurationBuckets(buckets...)
}
