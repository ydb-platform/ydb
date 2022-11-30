package unistat

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StructuredAggregation provides type safe API to create an Aggregation. For more
// information see: https://wiki.yandex-team.ru/golovan/aggregation-types/
type StructuredAggregation struct {
	AggregationType AggregationType
	Group           AggregationRule
	MetaGroup       AggregationRule
	Rollup          AggregationRule
}

// Aggregation defines rules how to aggregate signal on each level. For more
// information see: https://wiki.yandex-team.ru/golovan/aggregation-types/
type Aggregation interface {
	Suffix() string
}

const (
	AggregationUnknown = "<unknown>"
)

// Suffix defines signal aggregation on each level:
// 1 - Signal type: absolute (A) or delta (D).
// 2 - Group aggregation.
// 3 - Meta-group aggregation type.
// 4 - Time aggregation for roll-up.
//
// Doc: https://doc.yandex-team.ru/Search/golovan-quickstart/concepts/signal-aggregation.html#agrr-levels
func (a StructuredAggregation) Suffix() string {
	return fmt.Sprintf("%s%s%s%s", a.AggregationType, a.Group, a.MetaGroup, a.Rollup)
}

// Priority is used to order signals in unistat report.
// https://wiki.yandex-team.ru/golovan/stat-handle/#protokol
type Priority int

// AggregationType is Absolute or Delta.
type AggregationType int

// Value types
const (
	Absolute AggregationType = iota // Absolute value. Use for gauges.
	Delta                           // Delta value. Use for increasing counters.
)

func (v AggregationType) String() string {
	switch v {
	case Absolute:
		return "a"
	case Delta:
		return "d"
	default:
		return AggregationUnknown
	}
}

// AggregationRule defines aggregation rules:
//
//	https://wiki.yandex-team.ru/golovan/aggregation-types/#algoritmyagregacii
type AggregationRule int

// Aggregation rules
const (
	Hgram   AggregationRule = iota // Hgram is histogram aggregation.
	Max                            // Max value.
	Min                            // Min value.
	Sum                            // Sum with default 0.
	SumNone                        // SumNone is sum with default None.
	Last                           // Last value.
	Average                        // Average value.
)

func (r AggregationRule) String() string {
	switch r {
	case Hgram:
		return "h"
	case Max:
		return "x"
	case Min:
		return "n"
	case Sum:
		return "m"
	case SumNone:
		return "e"
	case Last:
		return "t"
	case Average:
		return "v"
	default:
		return AggregationUnknown
	}
}

func (r *AggregationRule) UnmarshalText(source []byte) error {
	text := string(source)
	switch text {
	case "h":
		*r = Hgram
	case "x":
		*r = Max
	case "n":
		*r = Min
	case "m":
		*r = Sum
	case "e":
		*r = SumNone
	case "t":
		*r = Last
	case "v":
		*r = Average
	default:
		return fmt.Errorf("unknown aggregation rule '%s'", text)
	}
	return nil
}

// ErrDuplicate is raised on duplicate metric name registration.
var ErrDuplicate = errors.New("unistat: duplicate metric")

// Metric is interface that accepted by Registry.
type Metric interface {
	Name() string
	Priority() Priority
	Aggregation() Aggregation
	MarshalJSON() ([]byte, error)
}

// Updater is interface that wraps basic Update() method.
type Updater interface {
	Update(value float64)
}

// Registry is interface for container that generates stat report
type Registry interface {
	Register(metric Metric)
	MarshalJSON() ([]byte, error)
}

var defaultRegistry = NewRegistry()

// Register metric in default registry.
func Register(metric Metric) {
	defaultRegistry.Register(metric)
}

// MarshalJSON marshals default registry to JSON.
func MarshalJSON() ([]byte, error) {
	return json.Marshal(defaultRegistry)
}

// MeasureMicrosecondsSince updates metric with duration that started
// at ts and ends now.
func MeasureMicrosecondsSince(m Updater, ts time.Time) {
	measureMicrosecondsSince(time.Since, m, ts)
}

// For unittest
type timeSinceFunc func(t time.Time) time.Duration

func measureMicrosecondsSince(sinceFunc timeSinceFunc, m Updater, ts time.Time) {
	dur := sinceFunc(ts)
	m.Update(float64(dur / time.Microsecond)) // to microseconds
}
