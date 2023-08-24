package asynczap

import (
	"bytes"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestCompareToDefault(t *testing.T) {
	var buf0, buf1 bytes.Buffer
	out0 := zapcore.AddSync(&buf0)
	out1 := zapcore.AddSync(&buf1)

	format := zap.NewProductionEncoderConfig()
	format.EncodeTime = func(t time.Time, e zapcore.PrimitiveArrayEncoder) {
		e.AppendString("10:00")
	}

	asyncCore := NewCore(
		zapcore.NewJSONEncoder(format),
		out0,
		zap.DebugLevel,
		Options{})

	log0 := zap.New(asyncCore)
	log0.Error("foo")

	require.NoError(t, asyncCore.Sync())
	asyncCore.Stop()

	syncCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(format),
		out1,
		zap.DebugLevel)

	log1 := zap.New(syncCore)
	log1.Error("foo")

	require.Equal(t, buf0.String(), buf1.String())
}

type countWriteSyncer int32

func (c *countWriteSyncer) Write(b []byte) (int, error) {
	atomic.AddInt32((*int32)(c), 1)
	return len(b), nil
}

func (c *countWriteSyncer) Sync() error {
	return nil
}

func TestSync(t *testing.T) {
	var c countWriteSyncer
	out0 := &c

	format := zap.NewProductionEncoderConfig()
	format.EncodeTime = func(t time.Time, e zapcore.PrimitiveArrayEncoder) {
		e.AppendString("10:00")
	}

	asyncCore := NewCore(
		zapcore.NewJSONEncoder(format),
		out0,
		zap.DebugLevel,
		Options{FlushInterval: 10 * time.Nanosecond})

	log0 := zap.New(asyncCore)

	for i := 0; i < 100000; i++ {
		log0.Error("123")
		_ = log0.Sync()
		require.EqualValues(t, i+1, atomic.LoadInt32((*int32)(&c)))
	}
}

type lockWriter struct {
	c chan struct{}
}

func (w *lockWriter) Write(b []byte) (int, error) {
	<-w.c
	return 0, nil
}

func TestDropsRecordsOnOverflow(t *testing.T) {
	go func() {
		time.Sleep(time.Second * 15)

		buf := make([]byte, 1024*1024)
		n := runtime.Stack(buf, true)
		_, _ = os.Stderr.Write(buf[:n])
	}()

	w := &lockWriter{c: make(chan struct{})}

	asyncCore := NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(w),
		zap.DebugLevel,
		Options{
			MaxMemoryUsage: 100,
		})
	defer asyncCore.Stop()

	log := zap.New(asyncCore)

	for i := 0; i < 1000; i++ {
		log.Error("foobar", zap.String("key", strings.Repeat("x", 1000)))
	}

	assert.Greater(t, asyncCore.Stat().DroppedRecords, 990)
	close(w.c)
}
