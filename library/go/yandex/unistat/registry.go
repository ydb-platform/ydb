package unistat

import (
	"encoding/json"
	"sort"
	"sync"
)

type registry struct {
	mu     sync.Mutex
	byName map[string]Metric

	metrics  []Metric
	unsorted bool
}

// NewRegistry allocate new registry container for unistat metrics.
func NewRegistry() Registry {
	return &registry{
		byName:  map[string]Metric{},
		metrics: []Metric{},
	}
}

func (r *registry) Register(m Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.byName[m.Name()]; ok {
		panic(ErrDuplicate)
	}

	r.byName[m.Name()] = m
	r.metrics = append(r.metrics, m)
	r.unsorted = true
}

func (r *registry) MarshalJSON() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.unsorted {
		sort.Sort(byPriority(r.metrics))
		r.unsorted = false
	}
	return json.Marshal(r.metrics)
}

type byPriority []Metric

func (m byPriority) Len() int { return len(m) }
func (m byPriority) Less(i, j int) bool {
	if m[i].Priority() == m[j].Priority() {
		return m[i].Name() < m[j].Name()
	}

	return m[i].Priority() > m[j].Priority()
}
func (m byPriority) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
