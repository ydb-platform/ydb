package unistat

import (
	"encoding/json"
	"math"
	"sync"
)

// Numeric implements Metric interface.
type Numeric struct {
	mu        sync.RWMutex
	name      string
	priority  Priority
	aggr      Aggregation
	localAggr AggregationRule

	value float64
}

// NewNumeric allocates Numeric value metric.
func NewNumeric(name string, priority Priority, aggr Aggregation, localAggr AggregationRule) *Numeric {
	return &Numeric{
		name:      name,
		priority:  priority,
		aggr:      aggr,
		localAggr: localAggr,
	}
}

// Name from Metric interface.
func (n *Numeric) Name() string {
	return n.name
}

// Aggregation from Metric interface.
func (n *Numeric) Aggregation() Aggregation {
	return n.aggr
}

// Priority from Metric interface.
func (n *Numeric) Priority() Priority {
	return n.priority
}

// Update from Metric interface.
func (n *Numeric) Update(value float64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.localAggr {
	case Max:
		n.value = math.Max(n.value, value)
	case Min:
		n.value = math.Min(n.value, value)
	case Sum:
		n.value += value
	case Last:
		n.value = value
	default:
		n.value = -1
	}
}

// MarshalJSON from Metric interface.
func (n *Numeric) MarshalJSON() ([]byte, error) {
	jsonName := n.name + "_" + n.aggr.Suffix()
	return json.Marshal([]interface{}{jsonName, n.GetValue()})
}

// GetValue returns current metric value.
func (n *Numeric) GetValue() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.value
}

// SetValue sets current metric value.
func (n *Numeric) SetValue(value float64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.value = value
}
