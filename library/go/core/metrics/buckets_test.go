package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewDurationBuckets(t *testing.T) {
	buckets := []time.Duration{
		1 * time.Second,
		3 * time.Second,
		5 * time.Second,
	}
	bk := NewDurationBuckets(buckets...)

	expect := durationBuckets{
		buckets: []time.Duration{
			1 * time.Second,
			3 * time.Second,
			5 * time.Second,
		},
	}
	assert.Equal(t, expect, bk)
}

func Test_durationBuckets_MapDuration(t *testing.T) {
	bk := NewDurationBuckets([]time.Duration{
		1 * time.Second,
		3 * time.Second,
		5 * time.Second,
	}...)

	for i := 0; i <= bk.Size(); i++ {
		assert.Equal(t, i, bk.MapDuration(time.Duration(i*2)*time.Second))
	}
}

func Test_durationBuckets_Size(t *testing.T) {
	var buckets []time.Duration
	for i := 1; i < 3; i++ {
		buckets = append(buckets, time.Duration(i)*time.Second)
		bk := NewDurationBuckets(buckets...)
		assert.Equal(t, i, bk.Size())
	}
}

func Test_durationBuckets_UpperBound(t *testing.T) {
	bk := NewDurationBuckets([]time.Duration{
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
	}...)

	assert.Panics(t, func() { bk.UpperBound(999) })

	for i := 0; i < bk.Size()-1; i++ {
		assert.Equal(t, time.Duration(i+1)*time.Second, bk.UpperBound(i))
	}
}

func TestNewBuckets(t *testing.T) {
	bk := NewBuckets(1, 3, 5)

	expect := buckets{
		buckets: []float64{1, 3, 5},
	}
	assert.Equal(t, expect, bk)
}

func Test_buckets_MapValue(t *testing.T) {
	bk := NewBuckets(1, 3, 5)

	for i := 0; i <= bk.Size(); i++ {
		assert.Equal(t, i, bk.MapValue(float64(i*2)))
	}
}

func Test_buckets_Size(t *testing.T) {
	var buckets []float64
	for i := 1; i < 3; i++ {
		buckets = append(buckets, float64(i))
		bk := NewBuckets(buckets...)
		assert.Equal(t, i, bk.Size())
	}
}

func Test_buckets_UpperBound(t *testing.T) {
	bk := NewBuckets(1, 2, 3)

	assert.Panics(t, func() { bk.UpperBound(999) })

	for i := 0; i < bk.Size()-1; i++ {
		assert.Equal(t, float64(i+1), bk.UpperBound(i))
	}
}

func TestMakeLinearBuckets_CorrectParameters_NotPanics(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Equal(t,
			NewBuckets(0.0, 1.0, 2.0),
			MakeLinearBuckets(0, 1, 3),
		)
	})
}

func TestMakeLinearBucketsPanicsOnBadCount(t *testing.T) {
	assert.Panics(t, func() {
		MakeLinearBuckets(0, 1, 0)
	})
}

func TestMakeLinearDurationBuckets(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Equal(t,
			NewDurationBuckets(0, time.Second, 2*time.Second),
			MakeLinearDurationBuckets(0*time.Second, 1*time.Second, 3),
		)
	})
}

func TestMakeLinearDurationBucketsPanicsOnBadCount(t *testing.T) {
	assert.Panics(t, func() {
		MakeLinearDurationBuckets(0*time.Second, 1*time.Second, 0)
	})
}

func TestMakeExponentialBuckets(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Equal(
			t,
			NewBuckets(2, 4, 8),
			MakeExponentialBuckets(2, 2, 3),
		)
	})
}

func TestMakeExponentialBucketsPanicsOnBadCount(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialBuckets(2, 2, 0)
	})
}

func TestMakeExponentialBucketsPanicsOnBadStart(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialBuckets(0, 2, 2)
	})
}

func TestMakeExponentialBucketsPanicsOnBadFactor(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialBuckets(2, 1, 2)
	})
}

func TestMakeExponentialDurationBuckets(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Equal(
			t,
			NewDurationBuckets(2*time.Second, 4*time.Second, 8*time.Second),
			MakeExponentialDurationBuckets(2*time.Second, 2, 3),
		)
	})
}

func TestMakeExponentialDurationBucketsPanicsOnBadCount(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialDurationBuckets(2*time.Second, 2, 0)
	})
}

func TestMakeExponentialDurationBucketsPanicsOnBadStart(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialDurationBuckets(0, 2, 2)
	})
}

func TestMakeExponentialDurationBucketsPanicsOnBadFactor(t *testing.T) {
	assert.Panics(t, func() {
		MakeExponentialDurationBuckets(2*time.Second, 1, 2)
	})
}
