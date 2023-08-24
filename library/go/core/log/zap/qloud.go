package zap

import (
	"github.com/ydb-platform/ydb/library/go/core/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewQloudLogger constructs fully-fledged Qloud compatible logger
// based on predefined config. See https://wiki.yandex-team.ru/qloud/doc/logs
// for more information
func NewQloudLogger(level log.Level, opts ...zap.Option) (*Logger, error) {
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(ZapifyLevel(level)),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			StacktraceKey:  "stackTrace",
			TimeKey:        "",
			CallerKey:      "",
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}

	zl, err := cfg.Build(opts...)
	if err != nil {
		return nil, err
	}

	return &Logger{
		L: addQloudContext(zl).(*zap.Logger),
	}, nil
}

func addQloudContext(i interface{}) interface{} {
	switch c := i.(type) {
	case *zap.Logger:
		return c.With(zap.Namespace("@fields"))
	case zapcore.Core:
		return c.With([]zapcore.Field{zap.Namespace("@fields")})
	}
	return i
}
