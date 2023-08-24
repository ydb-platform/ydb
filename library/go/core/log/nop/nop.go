package nop

import (
	"os"

	"github.com/ydb-platform/ydb/library/go/core/log"
)

// Logger that does nothing
type Logger struct{}

var _ log.Logger = &Logger{}
var _ log.Structured = &Logger{}
var _ log.Fmt = &Logger{}

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

// Trace implements Trace method of log.Logger interface
func (l *Logger) Trace(msg string, fields ...log.Field) {}

// Tracef implements Tracef method of log.Logger interface
func (l *Logger) Tracef(format string, args ...interface{}) {}

// Debug implements Debug method of log.Logger interface
func (l *Logger) Debug(msg string, fields ...log.Field) {}

// Debugf implements Debugf method of log.Logger interface
func (l *Logger) Debugf(format string, args ...interface{}) {}

// Info implements Info method of log.Logger interface
func (l *Logger) Info(msg string, fields ...log.Field) {}

// Infof implements Infof method of log.Logger interface
func (l *Logger) Infof(format string, args ...interface{}) {}

// Warn implements Warn method of log.Logger interface
func (l *Logger) Warn(msg string, fields ...log.Field) {}

// Warnf implements Warnf method of log.Logger interface
func (l *Logger) Warnf(format string, args ...interface{}) {}

// Error implements Error method of log.Logger interface
func (l *Logger) Error(msg string, fields ...log.Field) {}

// Errorf implements Errorf method of log.Logger interface
func (l *Logger) Errorf(format string, args ...interface{}) {}

// Fatal implements Fatal method of log.Logger interface
func (l *Logger) Fatal(msg string, fields ...log.Field) {
	os.Exit(1)
}

// Fatalf implements Fatalf method of log.Logger interface
func (l *Logger) Fatalf(format string, args ...interface{}) {
	os.Exit(1)
}

func (l *Logger) WithName(name string) log.Logger {
	return l
}
