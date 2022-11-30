package solomon

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"time"

	"a.yandex-team.ru/library/go/core/xerrors"
)

// Gather collects all metrics data via snapshots.
func (r Registry) Gather() (*Metrics, error) {
	metrics := make([]Metric, 0)

	var err error
	r.metrics.Range(func(_, v interface{}) bool {
		if s, ok := v.(Metric); ok {
			metrics = append(metrics, s.Snapshot())
			return true
		}
		err = fmt.Errorf("unexpected value type: %T", v)
		return false
	})

	if err != nil {
		return nil, err
	}

	return &Metrics{metrics: metrics}, nil
}

func NewMetrics(metrics []Metric) Metrics {
	return Metrics{metrics: metrics}
}

func NewMetricsWithTimestamp(metrics []Metric, ts time.Time) Metrics {
	return Metrics{metrics: metrics, timestamp: &ts}
}

type valueType uint8

const (
	valueTypeNone         valueType = iota
	valueTypeOneWithoutTS valueType = 0x01
	valueTypeOneWithTS    valueType = 0x02
	valueTypeManyWithTS   valueType = 0x03
)

type metricType uint8

const (
	typeUnspecified    metricType = iota
	typeGauge          metricType = 0x01
	typeCounter        metricType = 0x02
	typeRated          metricType = 0x03
	typeHistogram      metricType = 0x05
	typeRatedHistogram metricType = 0x06
)

func (k metricType) String() string {
	switch k {
	case typeCounter:
		return "COUNTER"
	case typeGauge:
		return "DGAUGE"
	case typeHistogram:
		return "HIST"
	case typeRated:
		return "RATE"
	case typeRatedHistogram:
		return "HIST_RATE"
	default:
		panic("unknown metric type")
	}
}

// Metric is an any abstract solomon Metric.
type Metric interface {
	json.Marshaler

	Name() string
	getType() metricType
	getLabels() map[string]string
	getValue() interface{}
	getNameTag() string
	getTimestamp() *time.Time

	Snapshot() Metric
}

// Rated marks given Solomon metric or vector as rated.
// Example:
//
//	cnt := r.Counter("mycounter")
//	Rated(cnt)
//
//	cntvec := r.CounterVec("mycounter", []string{"mytag"})
//	Rated(cntvec)
//
// For additional info: https://docs.yandex-team.ru/solomon/data-collection/dataformat/json
func Rated(s interface{}) {
	switch st := s.(type) {
	case *Counter:
		st.metricType = typeRated
	case *FuncCounter:
		st.metricType = typeRated
	case *Histogram:
		st.metricType = typeRatedHistogram

	case *CounterVec:
		st.vec.rated = true
	case *HistogramVec:
		st.vec.rated = true
	case *DurationHistogramVec:
		st.vec.rated = true
	}
	// any other metrics types are unrateable
}

var (
	_ json.Marshaler           = (*Metrics)(nil)
	_ encoding.BinaryMarshaler = (*Metrics)(nil)
)

type Metrics struct {
	metrics   []Metric
	timestamp *time.Time
}

// MarshalJSON implements json.Marshaler.
func (s Metrics) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Metrics   []Metric `json:"metrics"`
		Timestamp *int64   `json:"ts,omitempty"`
	}{s.metrics, tsAsRef(s.timestamp)})
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (s Metrics) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	se := NewSpackEncoder(context.Background(), CompressionNone, &s)
	n, err := se.Encode(&buf)
	if err != nil {
		return nil, xerrors.Errorf("encode only %d bytes: %w", n, err)
	}
	return buf.Bytes(), nil
}

// SplitToChunks splits Metrics into a slice of chunks, each at most maxChunkSize long.
// The length of returned slice is always at least one.
// Zero maxChunkSize denotes unlimited chunk length.
func (s Metrics) SplitToChunks(maxChunkSize int) []Metrics {
	if maxChunkSize == 0 || len(s.metrics) == 0 {
		return []Metrics{s}
	}
	chunks := make([]Metrics, 0, len(s.metrics)/maxChunkSize+1)

	for leftBound := 0; leftBound < len(s.metrics); leftBound += maxChunkSize {
		rightBound := leftBound + maxChunkSize
		if rightBound > len(s.metrics) {
			rightBound = len(s.metrics)
		}
		chunk := s.metrics[leftBound:rightBound]
		chunks = append(chunks, Metrics{metrics: chunk})
	}
	return chunks
}

func tsAsRef(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	ts := t.Unix()
	return &ts
}
