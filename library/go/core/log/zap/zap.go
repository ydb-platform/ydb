package zap

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap/encoders"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// callerSkip is number of stack frames to skip when logging caller
	callerSkip = 1
)

func init() {
	if err := zap.RegisterEncoder(encoders.EncoderNameKV, encoders.NewKVEncoder); err != nil {
		panic(err)
	}
	if err := zap.RegisterEncoder(encoders.EncoderNameCli, encoders.NewCliEncoder); err != nil {
		panic(err)
	}
	if err := zap.RegisterEncoder(encoders.EncoderNameTSKV, encoders.NewTSKVEncoder); err != nil {
		panic(err)
	}
}

// Logger implements log.Logger interface
type Logger struct {
	L *zap.Logger
}

var _ log.Logger = &Logger{}
var _ log.Structured = &Logger{}
var _ log.Fmt = &Logger{}
var _ log.LoggerWith = &Logger{}
var _ log.LoggerAddCallerSkip = &Logger{}

// New constructs zap-based logger from provided config
func New(cfg zap.Config) (*Logger, error) {
	zl, err := cfg.Build(zap.AddCallerSkip(callerSkip))
	if err != nil {
		return nil, err
	}

	return &Logger{
		L: zl,
	}, nil
}

// NewWithCore constructs zap-based logger from provided core
func NewWithCore(core zapcore.Core, options ...zap.Option) *Logger {
	options = append(options, zap.AddCallerSkip(callerSkip))
	return &Logger{L: zap.New(core, options...)}
}

// Must constructs zap-based logger from provided config and panics on error
func Must(cfg zap.Config) *Logger {
	l, err := New(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to construct zap logger: %v", err))
	}
	return l
}

// JSONConfig returns zap config for structured logging (zap's json encoder)
func JSONConfig(level log.Level) zap.Config {
	return StandardConfig("json", level)
}

// ConsoleConfig returns zap config for logging to console (zap's console encoder)
func ConsoleConfig(level log.Level) zap.Config {
	return StandardConfig("console", level)
}

// CLIConfig returns zap config for cli logging (custom cli encoder)
func CLIConfig(level log.Level) zap.Config {
	return StandardConfig("cli", level)
}

// KVConfig returns zap config for logging to kv (custom kv encoder)
func KVConfig(level log.Level) zap.Config {
	return StandardConfig("kv", level)
}

// TSKVConfig returns zap config for logging to tskv (custom tskv encoder)
func TSKVConfig(level log.Level) zap.Config {
	return zap.Config{
		Level:            zap.NewAtomicLevelAt(ZapifyLevel(level)),
		Encoding:         "tskv",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "message",
			LevelKey:       "levelname",
			TimeKey:        "unixtime",
			CallerKey:      "caller",
			NameKey:        "name",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.EpochTimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}
}

// StandardConfig returns default zap config with specified encoding and level
func StandardConfig(encoding string, level log.Level) zap.Config {
	return zap.Config{
		Level:            zap.NewAtomicLevelAt(ZapifyLevel(level)),
		Encoding:         encoding,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			NameKey:        "name",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}
}

// Logger returns general logger
func (l *Logger) Logger() log.Logger {
	return l
}

// Fmt returns fmt logger
func (l *Logger) Fmt() log.Fmt {
	return l
}

// Structured returns structured logger
func (l *Logger) Structured() log.Structured {
	return l
}

// With returns logger that always adds provided key/value to every log entry
func (l *Logger) With(fields ...log.Field) log.Logger {
	return &Logger{
		L: l.L.With(zapifyFields(fields...)...),
	}
}

func (l *Logger) AddCallerSkip(skip int) log.Logger {
	return &Logger{
		L: l.L.WithOptions(zap.AddCallerSkip(skip)),
	}
}

// Trace logs at Trace log level using fields
func (l *Logger) Trace(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.DebugLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Tracef logs at Trace log level using fmt formatter
func (l *Logger) Tracef(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.DebugLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// Debug logs at Debug log level using fields
func (l *Logger) Debug(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.DebugLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Debugf logs at Debug log level using fmt formatter
func (l *Logger) Debugf(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.DebugLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// Info logs at Info log level using fields
func (l *Logger) Info(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.InfoLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Infof logs at Info log level using fmt formatter
func (l *Logger) Infof(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.InfoLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// Warn logs at Warn log level using fields
func (l *Logger) Warn(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.WarnLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Warnf logs at Warn log level using fmt formatter
func (l *Logger) Warnf(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.WarnLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// Error logs at Error log level using fields
func (l *Logger) Error(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.ErrorLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Errorf logs at Error log level using fmt formatter
func (l *Logger) Errorf(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.ErrorLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// Fatal logs at Fatal log level using fields
func (l *Logger) Fatal(msg string, fields ...log.Field) {
	if ce := l.L.Check(zap.FatalLevel, msg); ce != nil {
		ce.Write(zapifyFields(fields...)...)
	}
}

// Fatalf logs at Fatal log level using fmt formatter
func (l *Logger) Fatalf(msg string, args ...interface{}) {
	if ce := l.L.Check(zap.FatalLevel, ""); ce != nil {
		ce.Message = fmt.Sprintf(msg, args...)
		ce.Write()
	}
}

// WithName adds name to logger
func (l *Logger) WithName(name string) log.Logger {
	return &Logger{
		L: l.L.Named(name),
	}
}
