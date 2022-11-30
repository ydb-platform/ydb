package zap

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"a.yandex-team.ru/library/go/core/log"
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

// NewDeployConfig returns default configuration (with no sampling).
// Not recommended for production use.
func NewDeployConfig() zap.Config {
	return zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    NewDeployEncoderConfig(),
	}
}

// NewDeployLogger constructs fully-fledged Deploy compatible logger
// based on predefined config. See https://deploy.yandex-team.ru/docs/concepts/pod/sidecars/logs/logs#format
// for more information
func NewDeployLogger(level log.Level, opts ...zap.Option) (*Logger, error) {
	cfg := NewDeployConfig()
	cfg.Level = zap.NewAtomicLevelAt(ZapifyLevel(level))

	zl, err := cfg.Build(opts...)
	if err != nil {
		return nil, err
	}

	return &Logger{
		L: addDeployContext(zl).(*zap.Logger),
	}, nil
}

// NewProductionDeployConfig returns configuration, suitable for production use.
//
// It uses a JSON encoder, writes to standard error, and enables sampling.
// Stacktraces are automatically included on logs of ErrorLevel and above.
func NewProductionDeployConfig() zap.Config {
	return zap.Config{
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Development:      false,
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    NewDeployEncoderConfig(),
	}
}

// Same as NewDeployLogger, but with sampling
func NewProductionDeployLogger(level log.Level, opts ...zap.Option) (*Logger, error) {
	cfg := NewProductionDeployConfig()
	cfg.Level = zap.NewAtomicLevelAt(ZapifyLevel(level))

	zl, err := cfg.Build(opts...)
	if err != nil {
		return nil, err
	}

	return &Logger{
		L: addDeployContext(zl).(*zap.Logger),
	}, nil
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
