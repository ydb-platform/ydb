package solomon

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"sort"
	"sync"
	"time"

	"go.uber.org/atomic"

	"a.yandex-team.ru/library/go/core/metrics"
	"a.yandex-team.ru/library/go/core/xerrors"
)

var (
	_ metrics.Histogram = (*Histogram)(nil)
	_ metrics.Timer     = (*Histogram)(nil)
	_ Metric            = (*Histogram)(nil)
)

type Histogram struct {
	name         string
	metricType   metricType
	tags         map[string]string
	bucketBounds []float64
	bucketValues []int64
	infValue     atomic.Int64
	mutex        sync.Mutex
	timestamp    *time.Time
	useNameTag   bool
}

type histogram struct {
	Bounds  []float64 `json:"bounds"`
	Buckets []int64   `json:"buckets"`
	Inf     int64     `json:"inf,omitempty"`
}

func (h *histogram) writeHistogram(w io.Writer) error {
	err := writeULEB128(w, uint32(len(h.Buckets)))
	if err != nil {
		return xerrors.Errorf("writeULEB128 size histogram buckets failed: %w", err)
	}

	for _, upperBound := range h.Bounds {
		err = binary.Write(w, binary.LittleEndian, float64(upperBound))
		if err != nil {
			return xerrors.Errorf("binary.Write upper bound failed: %w", err)
		}
	}

	for _, bucketValue := range h.Buckets {
		err = binary.Write(w, binary.LittleEndian, uint64(bucketValue))
		if err != nil {
			return xerrors.Errorf("binary.Write histogram buckets failed: %w", err)
		}
	}
	return nil
}

func (h *Histogram) RecordValue(value float64) {
	boundIndex := sort.SearchFloat64s(h.bucketBounds, value)

	if boundIndex < len(h.bucketValues) {
		h.mutex.Lock()
		h.bucketValues[boundIndex] += 1
		h.mutex.Unlock()
	} else {
		h.infValue.Inc()
	}
}

func (h *Histogram) RecordDuration(value time.Duration) {
	h.RecordValue(value.Seconds())
}

func (h *Histogram) Reset() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.bucketValues = make([]int64, len(h.bucketValues))
	h.infValue.Store(0)
}

func (h *Histogram) Name() string {
	return h.name
}

func (h *Histogram) getType() metricType {
	return h.metricType
}

func (h *Histogram) getLabels() map[string]string {
	return h.tags
}

func (h *Histogram) getValue() interface{} {
	return histogram{
		Bounds:  h.bucketBounds,
		Buckets: h.bucketValues,
	}
}

func (h *Histogram) getTimestamp() *time.Time {
	return h.timestamp
}

func (h *Histogram) getNameTag() string {
	if h.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

// MarshalJSON implements json.Marshaler.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Histogram histogram         `json:"hist"`
		Timestamp *int64            `json:"ts,omitempty"`
	}{
		Type: h.metricType.String(),
		Histogram: histogram{
			Bounds:  h.bucketBounds,
			Buckets: h.bucketValues,
			Inf:     h.infValue.Load(),
		},
		Labels: func() map[string]string {
			labels := make(map[string]string, len(h.tags)+1)
			labels[h.getNameTag()] = h.Name()
			for k, v := range h.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(h.timestamp),
	})
}

// Snapshot returns independent copy on metric.
func (h *Histogram) Snapshot() Metric {
	bucketBounds := make([]float64, len(h.bucketBounds))
	bucketValues := make([]int64, len(h.bucketValues))

	copy(bucketBounds, h.bucketBounds)
	copy(bucketValues, h.bucketValues)

	return &Histogram{
		name:         h.name,
		metricType:   h.metricType,
		tags:         h.tags,
		bucketBounds: bucketBounds,
		bucketValues: bucketValues,
		infValue:     *atomic.NewInt64(h.infValue.Load()),
		useNameTag:   h.useNameTag,
	}
}

// InitBucketValues cleans internal bucketValues and saves new values in order.
// Length of internal bucketValues stays unchanged.
// If length of slice in argument bucketValues more than length of internal one,
// the first extra element of bucketValues is stored in infValue.
func (h *Histogram) InitBucketValues(bucketValues []int64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.bucketValues = make([]int64, len(h.bucketValues))
	h.infValue.Store(0)
	copy(h.bucketValues, bucketValues)
	if len(bucketValues) > len(h.bucketValues) {
		h.infValue.Store(bucketValues[len(h.bucketValues)])
	}
}
