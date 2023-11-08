package mock

import (
	"sort"
	"sync"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var (
	_ metrics.Histogram = (*Histogram)(nil)
	_ metrics.Timer     = (*Histogram)(nil)
)

type Histogram struct {
	Name         string
	Tags         map[string]string
	BucketBounds []float64
	BucketValues []int64
	InfValue     *atomic.Int64
	mutex        sync.Mutex
}

func (h *Histogram) RecordValue(value float64) {
	boundIndex := sort.SearchFloat64s(h.BucketBounds, value)

	if boundIndex < len(h.BucketValues) {
		h.mutex.Lock()
		h.BucketValues[boundIndex] += 1
		h.mutex.Unlock()
	} else {
		h.InfValue.Inc()
	}
}

func (h *Histogram) RecordDuration(value time.Duration) {
	h.RecordValue(value.Seconds())
}
