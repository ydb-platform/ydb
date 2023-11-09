package nop

import "github.com/ydb-platform/ydb/library/go/core/metrics"

var _ metrics.Counter = (*Counter)(nil)

type Counter struct{}

func (Counter) Inc() {}

func (Counter) Add(_ int64) {}

var _ metrics.CounterVec = (*CounterVec)(nil)

type CounterVec struct{}

func (t CounterVec) With(_ map[string]string) metrics.Counter {
	return Counter{}
}

func (t CounterVec) Reset() {}

var _ metrics.FuncCounter = (*FuncCounter)(nil)

type FuncCounter struct {
	function func() int64
}

func (c FuncCounter) Function() func() int64 {
	return c.function
}
