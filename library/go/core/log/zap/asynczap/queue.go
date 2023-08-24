package asynczap

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap/buffer"
)

var entryPool sync.Pool

func newEntry() *entry {
	pooled := entryPool.Get()
	if pooled != nil {
		return pooled.(*entry)
	} else {
		return new(entry)
	}
}

func putEntry(e *entry) {
	entryPool.Put(e)
}

type entry struct {
	next *entry
	buf  *buffer.Buffer
}

type queue struct {
	size int64
	head unsafe.Pointer
}

func (q *queue) loadHead() *entry {
	return (*entry)(atomic.LoadPointer(&q.head))
}

func (q *queue) casHead(old, new *entry) (swapped bool) {
	return atomic.CompareAndSwapPointer(&q.head, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (q *queue) swapHead() *entry {
	return (*entry)(atomic.SwapPointer(&q.head, nil))
}

func (q *queue) loadSize() int64 {
	return atomic.LoadInt64(&q.size)
}

func (q *queue) enqueue(buf *buffer.Buffer) {
	e := newEntry()
	e.buf = buf

	atomic.AddInt64(&q.size, int64(buf.Cap()))
	for {
		e.next = q.loadHead()
		if q.casHead(e.next, e) {
			break
		}
	}
}

func (q *queue) dequeueAll(to []*buffer.Buffer) []*buffer.Buffer {
	head := q.swapHead()

	for head != nil {
		atomic.AddInt64(&q.size, -int64(head.buf.Cap()))
		to = append(to, head.buf)

		next := head.next
		putEntry(head)
		head = next
	}

	for i := 0; i < len(to)/2; i++ {
		j := len(to) - i - 1
		to[i], to[j] = to[j], to[i]
	}

	return to
}
