package inflight

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/x/xsync"
)

var _ metrics.CollectPolicy = (*inflightPolicy)(nil)

type inflightPolicy struct {
	addCollectLock sync.Mutex
	collect        atomic.Value // func(ctx context.Context)

	minUpdateInterval time.Duration
	lastUpdate        time.Time

	inflight xsync.SingleInflight
}

func NewCollectorPolicy(opts ...Option) metrics.CollectPolicy {
	c := &inflightPolicy{
		minUpdateInterval: time.Second,
		inflight:          xsync.NewSingleInflight(),
	}
	c.collect.Store(func(context.Context) {})

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (i *inflightPolicy) RegisteredCounter(counterFunc func() int64) func() int64 {
	return func() int64 {
		i.tryInflightUpdate()
		return counterFunc()
	}
}

func (i *inflightPolicy) RegisteredGauge(gaugeFunc func() float64) func() float64 {
	return func() float64 {
		i.tryInflightUpdate()
		return gaugeFunc()
	}
}

func (i *inflightPolicy) AddCollect(collect func(context.Context)) {
	oldCollect := i.getCollect()
	i.setCollect(func(ctx context.Context) {
		oldCollect(ctx)
		collect(ctx)
	})
}

func (i *inflightPolicy) tryInflightUpdate() {
	i.inflight.Do(func() {
		if time.Since(i.lastUpdate) < i.minUpdateInterval {
			return
		}

		i.getCollect()(context.Background())
		i.lastUpdate = time.Now()
	})
}

func (i *inflightPolicy) getCollect() func(context.Context) {
	return i.collect.Load().(func(context.Context))
}

func (i *inflightPolicy) setCollect(collect func(context.Context)) {
	i.collect.Store(collect)
}
