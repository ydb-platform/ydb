package zap

import (
	"github.com/ydb-platform/ydb/library/go/core/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewDeployEncoderConfig returns an opinionated EncoderConfig for
// deploy environment.
func NewDeployEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "levelStr",
		StacktraceKey:  "stackTrace",
		TimeKey:        "@timestamp",
		CallerKey:      "",
		NameKey:        "loggerName",
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

type cfgOption func(cfg *zap.Config)

// WithSampling sets sampling settings initial and thereafter
func WithSampling(initial int, thereafter int) cfgOption {
	return cfgOption(func(cfg *zap.Config) {
		cfg.Sampling = &zap.SamplingConfig{
			Initial:    initial,
			Thereafter: thereafter,
		}
	})
}

// SetOutputPaths sets OutputPaths (stdout by default)
func SetOutputPaths(paths []string) cfgOption {
	return cfgOption(func(cfg *zap.Config) {
		cfg.OutputPaths = paths
	})
}

// WithDevelopment sets Development option of zap.Config
func WithDevelopment(enabled bool) cfgOption {
	return cfgOption(func(cfg *zap.Config) {
		cfg.Development = enabled
	})
}

// WithLevel sets level of logging
func WithLevel(level log.Level) cfgOption {
	return cfgOption(func(cfg *zap.Config) {
		cfg.Level = zap.NewAtomicLevelAt(ZapifyLevel(level))
	})
}

// NewDeployConfig returns default configuration (with no sampling).
// Not recommended for production use.
func NewDeployConfig(opts ...cfgOption) zap.Config {
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    NewDeployEncoderConfig(),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// NewCustomDeployLogger constructs new logger by config cfg
func NewCustomDeployLogger(cfg zap.Config, opts ...zap.Option) (*Logger, error) {
	zl, err := cfg.Build(opts...)
	if err != nil {
		return nil, err
	}

	return &Logger{
		L: addDeployContext(zl).(*zap.Logger),
	}, nil
}

// NewDeployLogger constructs fully-fledged Deploy compatible logger
// based on predefined config. See https://deploy.yandex-team.ru/docs/concepts/pod/sidecars/logs/logs#format
// for more information
func NewDeployLogger(level log.Level, opts ...zap.Option) (*Logger, error) {
	return NewCustomDeployLogger(
		NewDeployConfig(
			WithLevel(level),
		),
		opts...,
	)
}

// NewProductionDeployConfig returns configuration, suitable for production use.
//
// It uses a JSON encoder, writes to standard error, and enables sampling.
// Stacktraces are automatically included on logs of ErrorLevel and above.
func NewProductionDeployConfig() zap.Config {
	return NewDeployConfig(
		WithDevelopment(false),
		WithSampling(100, 100),
	)
}

// Same as NewDeployLogger, but with sampling
func NewProductionDeployLogger(level log.Level, opts ...zap.Option) (*Logger, error) {
	return NewCustomDeployLogger(
		NewDeployConfig(
			WithLevel(level),
			WithDevelopment(false),
			WithSampling(100, 100),
		),
		opts...,
	)
}

func addDeployContext(i interface{}) interface{} {
	switch c := i.(type) {
	case *zap.Logger:
		return c.With(zap.Namespace("@fields"))
	case zapcore.Core:
		return c.With([]zapcore.Field{zap.Namespace("@fields")})
	}
	return i
}
