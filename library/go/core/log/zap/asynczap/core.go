// Package asynczap implements asynchronous core for zap.
//
// By default, zap writes every log line synchronously and without buffering. This behaviour
// is completely inadequate for high-rate logging.
//
// This implementation of zap.Core moves file write to background goroutine, while carefully
// monitoring memory consumption.
//
// When background goroutine can't keep up with logging rate, log records are dropped.
package asynczap

import (
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	Core struct {
		zapcore.LevelEnabler
		enc     zapcore.Encoder
		w       *background
		options Options
	}

	Stats struct {
		// Number of records dropped during memory overflow.
		DroppedRecords int

		// Number of errors returned from underlying writer.
		WriteErrors int
	}
)

// NewCore creates a Core that writes logs to a WriteSyncer.
func NewCore(enc zapcore.Encoder, ws zapcore.WriteSyncer, enab zapcore.LevelEnabler, options Options) *Core {
	options.setDefault()

	w := newBackground(options, ws)
	w.wg.Add(1)
	go w.run()

	return &Core{
		LevelEnabler: enab,
		enc:          enc,
		w:            w,
	}
}

func (c *Core) Stop() {
	_ = c.Sync()
	c.w.stop()
}

func (c *Core) With(fields []zap.Field) zapcore.Core {
	clone := c.clone()
	for i := range fields {
		fields[i].AddTo(clone.enc)
	}
	return clone
}

func (c *Core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *Core) Write(ent zapcore.Entry, fields []zap.Field) error {
	if size, ok, shouldReport := c.w.checkQueueSize(); !ok {
		if shouldReport {
			// Report overflow error only once per background iteration, to avoid spamming error output.
			return fmt.Errorf("logger queue overflow: %d >= %d", size, c.options.MaxMemoryUsage)
		} else {
			return nil
		}
	}

	buf, err := c.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}

	c.w.q.enqueue(buf)
	if ent.Level > zap.ErrorLevel {
		// Since we may be crashing the program, sync the output.
		_ = c.Sync()
	}
	return nil
}

func (c *Core) Sync() error {
	return c.w.sync()
}

func (c *Core) Stat() Stats {
	return Stats{
		DroppedRecords: int(atomic.LoadInt64(&c.w.droppedRecords)),
		WriteErrors:    int(atomic.LoadInt64(&c.w.writeErrors)),
	}
}

func (c *Core) clone() *Core {
	return &Core{
		LevelEnabler: c.LevelEnabler,
		enc:          c.enc.Clone(),
		w:            c.w,
		options:      c.options,
	}
}
