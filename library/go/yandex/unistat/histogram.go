package unistat

import (
	"encoding/json"
	"sync"
)

// Histogram implements Metric interface
type Histogram struct {
	mu       sync.RWMutex
	name     string
	priority Priority
	aggr     Aggregation

	intervals []float64
	weights   []int64
	size      int64
}

// NewHistogram allocates Histogram metric.
// For naming rules see https://wiki.yandex-team.ru/golovan/tagsandsignalnaming.
// Intervals in left edges of histograms buckets (maximum 50 allowed).
func NewHistogram(name string, priority Priority, aggr Aggregation, intervals []float64) *Histogram {
	return &Histogram{
		name:      name,
		priority:  priority,
		aggr:      aggr,
		intervals: intervals,
		weights:   make([]int64, len(intervals)),
	}
}

// Name from Metric interface.
func (h *Histogram) Name() string {
	return h.name
}

// Priority from Metric interface.
func (h *Histogram) Priority() Priority {
	return h.priority
}

// Aggregation from Metric interface.
func (h *Histogram) Aggregation() Aggregation {
	return h.aggr
}

// Update from Metric interface.
func (h *Histogram) Update(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := len(h.intervals); i > 0; i-- {
		if value >= h.intervals[i-1] {
			h.weights[i-1]++
			h.size++
			break
		}
	}
}

// MarshalJSON from Metric interface.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	buckets := [][2]interface{}{}
	for i := range h.intervals {
		b := h.intervals[i]
		w := h.weights[i]
		buckets = append(buckets, [2]interface{}{b, w})
	}

	jsonName := h.name + "_" + h.aggr.Suffix()
	return json.Marshal([]interface{}{jsonName, buckets})
}

// GetSize returns histogram's values count.
func (h *Histogram) GetSize() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.size
}
