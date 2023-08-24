package pion

import (
	"github.com/pion/logging"
	"github.com/ydb-platform/ydb/library/go/core/log"
)

type LoggerFactory struct {
	StandardLogger log.Logger
}

func (l LoggerFactory) NewLogger(scope string) logging.LeveledLogger {
	return LoggerAdapter{
		standardLogger: l.StandardLogger,
		scope:          scope,
	}
}

type LoggerAdapter struct {
	standardLogger log.Logger
	scope          string
}

func (a LoggerAdapter) Trace(msg string) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Trace(a.addScope(msg))
}

func (a LoggerAdapter) Tracef(format string, args ...interface{}) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Tracef(a.addScope(format), args...)
}

func (a LoggerAdapter) Debug(msg string) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Debug(a.addScope(msg))
}

func (a LoggerAdapter) Debugf(format string, args ...interface{}) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Debugf(a.addScope(format), args...)
}

func (a LoggerAdapter) Info(msg string) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Info(a.addScope(msg))
}

func (a LoggerAdapter) Infof(format string, args ...interface{}) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Infof(a.addScope(format), args...)
}

func (a LoggerAdapter) Warn(msg string) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Warn(a.addScope(msg))
}

func (a LoggerAdapter) Warnf(format string, args ...interface{}) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Warnf(a.addScope(format), args...)
}

func (a LoggerAdapter) Error(msg string) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Error(a.addScope(msg))
}

func (a LoggerAdapter) Errorf(format string, args ...interface{}) {
	log.AddCallerSkip(a.standardLogger, 1)
	a.standardLogger.Errorf(a.addScope(format), args...)
}

func (a LoggerAdapter) addScope(s string) string {
	return a.scope + ": " + s
}
