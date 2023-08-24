package asynczap

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
)

func BenchmarkQueue(b *testing.B) {
	var q queue

	go func() {
		var buf []*buffer.Buffer

		for range time.Tick(10 * time.Millisecond) {
			buf = q.dequeueAll(buf)
			buf = buf[:0]
		}
	}()

	p := &buffer.Buffer{}

	b.ReportAllocs()
	b.SetParallelism(runtime.NumCPU() - 1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.enqueue(p)
		}
	})
}

func TestQueue(t *testing.T) {
	var b0, b1, b2, b3 *buffer.Buffer
	b0 = &buffer.Buffer{}
	b0.AppendString("b0")
	b1 = &buffer.Buffer{}
	b1.AppendString("b1")
	b2 = &buffer.Buffer{}
	b2.AppendString("b2")
	b3 = &buffer.Buffer{}
	b3.AppendString("b3")

	var q queue
	q.enqueue(b0)
	q.enqueue(b1)
	q.enqueue(b2)

	require.Equal(t, []*buffer.Buffer{b0, b1, b2}, q.dequeueAll(nil))

	q.enqueue(b0)
	q.enqueue(b1)
	q.enqueue(b2)
	q.enqueue(b3)

	require.Equal(t, []*buffer.Buffer{b0, b1, b2, b3}, q.dequeueAll(nil))
}
