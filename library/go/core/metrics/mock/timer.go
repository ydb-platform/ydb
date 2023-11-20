package mock

import (
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"go.uber.org/atomic"
)

var _ metrics.Timer = (*Timer)(nil)

// Timer measures gauge duration.
type Timer struct {
	Name  string
	Tags  map[string]string
	Value *atomic.Duration
}

func (t *Timer) RecordDuration(value time.Duration) {
	t.Value.Store(value)
}
