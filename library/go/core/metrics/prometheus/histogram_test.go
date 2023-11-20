package prometheus

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/ptr"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestHistogram_RecordValue(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	h := rg.Histogram("test_histogram_record_value",
		metrics.NewBuckets(0.1, 1.0, 15.47, 42.0, 128.256),
	)

	for _, value := range []float64{0.5, 0.7, 34.1234, 127} {
		h.RecordValue(value)
	}

	expectBuckets := []*dto.Bucket{
		{CumulativeCount: ptr.Uint64(0), UpperBound: ptr.Float64(0.1)},
		{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(1.0)},
		{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(15.47)},
		{CumulativeCount: ptr.Uint64(3), UpperBound: ptr.Float64(42.0)},
		{CumulativeCount: ptr.Uint64(4), UpperBound: ptr.Float64(128.256)},
	}

	gathered, err := rg.Gather()
	require.NoError(t, err)

	resBuckets := gathered[0].Metric[0].GetHistogram().GetBucket()

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(),
		protocmp.Transform(),
	}
	assert.True(t, cmp.Equal(expectBuckets, resBuckets, cmpOpts...), cmp.Diff(expectBuckets, resBuckets, cmpOpts...))
}

func TestDurationHistogram_RecordDuration(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())

	ht := rg.DurationHistogram("test_histogram_record_value",
		metrics.NewDurationBuckets(
			1*time.Millisecond,                   // 0.1
			1*time.Second,                        // 1.0
			15*time.Second+470*time.Millisecond,  // 15.47
			42*time.Second,                       // 42.0
			128*time.Second+256*time.Millisecond, // 128.256
		),
	)

	values := []time.Duration{
		500 * time.Millisecond,
		700 * time.Millisecond,
		34*time.Second + 1234*time.Millisecond,
		127 * time.Second,
	}

	for _, value := range values {
		ht.RecordDuration(value)
	}

	expectBuckets := []*dto.Bucket{
		{CumulativeCount: ptr.Uint64(0), UpperBound: ptr.Float64(0.001)},
		{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(1)},
		{CumulativeCount: ptr.Uint64(2), UpperBound: ptr.Float64(15.47)},
		{CumulativeCount: ptr.Uint64(3), UpperBound: ptr.Float64(42)},
		{CumulativeCount: ptr.Uint64(4), UpperBound: ptr.Float64(128.256)},
	}

	gathered, err := rg.Gather()
	require.NoError(t, err)

	resBuckets := gathered[0].Metric[0].GetHistogram().GetBucket()

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(),
		protocmp.Transform(),
	}

	assert.True(t, cmp.Equal(expectBuckets, resBuckets, cmpOpts...), cmp.Diff(expectBuckets, resBuckets, cmpOpts...))
}
