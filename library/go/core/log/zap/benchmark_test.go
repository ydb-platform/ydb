package zap

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"go.uber.org/zap"
)

func BenchmarkZapLogger(b *testing.B) {
	// use config for both loggers
	cfg := NewDeployConfig()
	cfg.OutputPaths = nil
	cfg.ErrorOutputPaths = nil

	b.Run("stock", func(b *testing.B) {
		for _, level := range log.Levels() {
			b.Run(level.String(), func(b *testing.B) {
				cfg.Level = zap.NewAtomicLevelAt(ZapifyLevel(level))

				logger, err := cfg.Build()
				require.NoError(b, err)

				funcs := []func(string, ...zap.Field){
					logger.Debug,
					logger.Info,
					logger.Warn,
					logger.Error,
					logger.Fatal,
				}

				message := "test"
				fields := []zap.Field{
					zap.String("test", "test"),
					zap.Bool("test", true),
					zap.Int("test", 42),
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					funcs[i%(len(funcs)-1)](message, fields...)
				}
			})
		}
	})

	b.Run("wrapped", func(b *testing.B) {
		for _, level := range log.Levels() {
			b.Run(level.String(), func(b *testing.B) {
				cfg.Level = zap.NewAtomicLevelAt(ZapifyLevel(level))
				logger, err := New(cfg)
				require.NoError(b, err)

				funcs := []func(string, ...log.Field){
					logger.Debug,
					logger.Info,
					logger.Warn,
					logger.Error,
					logger.Fatal,
				}

				message := "test"
				fields := []log.Field{
					log.String("test", "test"),
					log.Bool("test", true),
					log.Int("test", 42),
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					funcs[i%(len(funcs)-1)](message, fields...)
				}
			})
		}
	})
}

func BenchmarkZapifyField(b *testing.B) {
	fields := []log.Field{
		log.Nil("test"),
		log.String("test", "test"),
		log.Binary("test", []byte("test")),
		log.Bool("test", true),
		log.Int("test", 42),
		log.UInt("test", 42),
		log.Float64("test", 42),
		log.Time("test", time.Now()),
		log.Duration("test", time.Second),
		log.NamedError("test", errors.New("test")),
		log.Strings("test", []string{"test"}),
		log.Any("test", "test"),
		log.Reflect("test", "test"),
		log.ByteString("test", []byte("test")),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zapifyField(fields[i%(len(fields)-1)])
	}
}

func BenchmarkZapifyFields(b *testing.B) {
	fields := []log.Field{
		log.Nil("test"),
		log.String("test", "test"),
		log.Binary("test", []byte("test")),
		log.Bool("test", true),
		log.Int("test", 42),
		log.UInt("test", 42),
		log.Float64("test", 42),
		log.Time("test", time.Now()),
		log.Duration("test", time.Second),
		log.NamedError("test", errors.New("test")),
		log.Strings("test", []string{"test"}),
		log.Any("test", "test"),
		log.Reflect("test", "test"),
		log.ByteString("test", []byte("test")),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zapifyFields(fields...)
	}
}
