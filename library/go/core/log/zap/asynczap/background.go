package asynczap

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// background is a single object shared by all clones of core.
type background struct {
	options Options

	q   queue
	out zapcore.WriteSyncer

	// use manual buffering instead of bufio background to preserve write atomicity.
	//
	// bufio.Writer might split log lines at arbitrary position.
	writeBuffer bytes.Buffer

	wg         sync.WaitGroup
	mu         sync.Mutex
	cond       *sync.Cond
	stopped    bool
	iter       int64
	lastErr    error
	forceFlush chan struct{}

	droppedRecords int64
	writeErrors    int64
	reportOverflow int64
}

func newBackground(options Options, out zapcore.WriteSyncer) *background {
	b := &background{
		options:    options,
		out:        out,
		forceFlush: make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *background) flush() {
	_, err := b.out.Write(b.writeBuffer.Bytes())
	if err != nil {
		b.onError(err)
	}
	b.writeBuffer.Reset()
}

func (b *background) onError(err error) {
	atomic.AddInt64(&b.writeErrors, 1)

	b.lastErr = err
}

func (b *background) stop() {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()

	b.wg.Wait()
}

func (b *background) finishIter() (stop bool) {
	b.mu.Lock()
	stop = b.stopped
	b.mu.Unlock()

	atomic.StoreInt64(&b.reportOverflow, 0)
	b.cond.Broadcast()
	return
}

func (b *background) run() {
	defer b.wg.Done()

	flush := time.NewTicker(b.options.FlushInterval)
	defer flush.Stop()

	var bufs []*buffer.Buffer
	for {
		bufs = bufs[:0]
		b.mu.Lock()

		bufs = b.q.dequeueAll(bufs)
		for _, buf := range bufs {
			b.writeBuffer.Write(buf.Bytes())
			buf.Free()

			if b.writeBuffer.Len() > b.options.WriteBufferSize {
				b.flush()
			}
		}

		if b.writeBuffer.Len() != 0 {
			b.flush()
		}

		b.iter++
		b.mu.Unlock()

		if b.finishIter() {
			return
		}

		select {
		case <-flush.C:
		case <-b.forceFlush:
			flush.Reset(b.options.FlushInterval)
		}

	}
}

func (b *background) checkQueueSize() (size int, ok, shouldReport bool) {
	size = int(b.q.loadSize())
	if size >= b.options.MaxMemoryUsage {
		atomic.AddInt64(&b.droppedRecords, 1)

		old := atomic.SwapInt64(&b.reportOverflow, 1)
		return size, false, old == 0
	}

	return 0, true, false
}

func (b *background) sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	select {
	case b.forceFlush <- struct{}{}:
	default:
	}

	now := b.iter
	for {
		if b.iter >= now+1 {
			return b.lastErr
		}

		if b.stopped {
			return errors.New("core has stopped")
		}

		b.cond.Wait()
	}
}
