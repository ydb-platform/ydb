package asynczap

import "time"

const (
	defaultMaxMemoryUsage  = 1 << 26 // 64MB
	defaultWriteBufferSize = 1 << 20 // 1MB
	defaultFlushInterval   = time.Millisecond * 100
)

type Options struct {
	// MaxMemoryUsage is maximum amount of memory that will be used by in-flight log records.
	MaxMemoryUsage int

	// WriteBufferSize specifies size of the buffer used for writes to underlying file.
	WriteBufferSize int

	// FlushInterval specifies how often background goroutine would wake up.
	FlushInterval time.Duration
}

func (o *Options) setDefault() {
	if o.MaxMemoryUsage == 0 {
		o.MaxMemoryUsage = defaultMaxMemoryUsage
	}

	if o.WriteBufferSize == 0 {
		o.WriteBufferSize = defaultWriteBufferSize
	}

	if o.FlushInterval == 0 {
		o.FlushInterval = defaultFlushInterval
	}
}
