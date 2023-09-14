package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/ctxlog"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
	"github.com/ydb-platform/ydb/library/go/core/log/zap/encoders"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type nopWriteSyncer struct{}

func (nws *nopWriteSyncer) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (nws *nopWriteSyncer) Sync() error {
	return nil
}

type levelEnabler struct {
	enabled bool
}

func (l *levelEnabler) Enabled(zapcore.Level) bool {
	return l.enabled
}

func BenchmarkWithFields(b *testing.B) {
	cfg := uberzap.NewDevelopmentEncoderConfig()
	cfg.TimeKey = ""
	cfg.LevelKey = ""
	kvEnc, _ := encoders.NewKVEncoder(cfg)
	enabler := &levelEnabler{}
	core := zapcore.NewCore(kvEnc, &nopWriteSyncer{}, enabler)
	logger := uberzap.New(core)
	ctx := ctxlog.WithFields(context.Background(),
		log.String("foo", "bar"),
		log.String("bar", "baz"),
	)

	l := &zap.Logger{L: logger}
	for _, enabled := range []bool{true, false} {
		enabler.enabled = enabled
		b.Run(fmt.Sprintf("zap_%t", enabled), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				logger.Info("test", uberzap.String("foo", "bar"), uberzap.String("bar", "baz"))
			}
		})
		b.Run(fmt.Sprintf("core_log_%t", enabled), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctxlog.Info(ctx, l, "test", log.String("foo", "bar"), log.String("bar", "baz"))
			}
		})
		b.Run(fmt.Sprintf("core_log_new_%t", enabled), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				l.Info("test", log.String("foo", "bar"), log.String("bar", "baz"), log.Context(ctx))
			}
		})
		b.Run(fmt.Sprintf("core_log_fmt_%t", enabled), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctxlog.Infof(ctx, l, "test %s %d", "test", 42)
			}
		})
	}

}
