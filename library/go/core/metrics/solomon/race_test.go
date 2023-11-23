package solomon

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

type spinBarrier struct {
	count   int64
	waiting atomic.Int64
	step    atomic.Int64
}

func newSpinBarrier(size int) *spinBarrier {
	return &spinBarrier{count: int64(size)}
}

func (b *spinBarrier) wait() {
	s := b.step.Load()
	w := b.waiting.Add(1)
	if w == b.count {
		b.waiting.Store(0)
		b.step.Add(1)
	} else {
		for s == b.step.Load() {
			// noop
		}
	}
}

func TestRaceDurationHistogramVecVersusStreamJson(t *testing.T) {
	// Regression test: https://github.com/ydb-platform/ydb/review/2690822/details
	registry := NewRegistry(NewRegistryOpts())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const stepCount = 200

	barrier := newSpinBarrier(2)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// Consumer
		defer wg.Done()
		out := bytes.NewBuffer(nil)
		for i := 0; i < stepCount; i++ {
			out.Reset()
			barrier.wait()
			_, err := registry.StreamJSON(ctx, out)
			if err != nil {
				require.ErrorIs(t, err, context.Canceled)
				break
			}
		}
	}()

	wg.Add(1)
	go func() {
		// Producer
		defer wg.Done()

		const success = "success"
		const version = "version"
		vecs := make([]metrics.TimerVec, 0)
		buckets := metrics.NewDurationBuckets(1, 2, 3)
	ProducerLoop:
		for i := 0; i < stepCount; i++ {
			barrier.wait()
			vec := registry.DurationHistogramVec(
				fmt.Sprintf("latency-%v", i),
				buckets,
				[]string{success, version},
			)
			Rated(vec)
			vecs = append(vecs, vec)
			for _, v := range vecs {
				v.With(map[string]string{success: "ok", version: "123"}).RecordDuration(time.Second)
				v.With(map[string]string{success: "false", version: "123"}).RecordDuration(time.Millisecond)
			}
			select {
			case <-ctx.Done():
				break ProducerLoop
			default:
				// noop
			}
		}
	}()
	wg.Wait()
}

func TestRaceDurationHistogramRecordDurationVersusStreamJson(t *testing.T) {
	// Regression test: https://github.com/ydb-platform/ydb/review/2690822/details

	registry := NewRegistry(NewRegistryOpts())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const stepCount = 200
	barrier := newSpinBarrier(2)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		// Consumer
		defer wg.Done()
		out := bytes.NewBuffer(nil)
		for i := 0; i < stepCount; i++ {
			out.Reset()
			barrier.wait()
			_, err := registry.StreamJSON(ctx, out)
			if err != nil {
				require.ErrorIs(t, err, context.Canceled)
				break
			}
		}
	}()

	wg.Add(1)
	go func() {
		// Producer
		defer wg.Done()

		buckets := metrics.NewDurationBuckets(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		hist := registry.DurationHistogram("latency", buckets)
		// Rated(hist)

	ProducerLoop:
		for i := 0; i < stepCount; i++ {
			barrier.wait()
			hist.RecordDuration(time.Duration(i % 10))
			select {
			case <-ctx.Done():
				break ProducerLoop
			default:
				// noop
			}
		}
	}()
	wg.Wait()
}
