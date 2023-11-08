package nop

import (
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

var _ metrics.Timer = (*Timer)(nil)

type Timer struct{}

func (Timer) RecordDuration(_ time.Duration) {}

var _ metrics.TimerVec = (*TimerVec)(nil)

type TimerVec struct{}

func (t TimerVec) With(_ map[string]string) metrics.Timer {
	return Timer{}
}

func (t TimerVec) Reset() {}
