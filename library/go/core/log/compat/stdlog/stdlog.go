package stdlog

import (
	"bytes"
	"fmt"
	stdlog "log"

	"github.com/ydb-platform/ydb/library/go/core/log"
)

func levelToFunc(logger log.Logger, lvl log.Level) (func(msg string, fields ...log.Field), error) {
	switch lvl {
	case log.DebugLevel:
		return logger.Debug, nil
	case log.TraceLevel:
		return logger.Trace, nil
	case log.InfoLevel:
		return logger.Info, nil
	case log.WarnLevel:
		return logger.Warn, nil
	case log.ErrorLevel:
		return logger.Error, nil
	case log.FatalLevel:
		return logger.Fatal, nil
	}

	return nil, fmt.Errorf("unknown log level: %v", lvl)
}

type loggerWriter struct {
	logFunc func(msg string, fields ...log.Field)
}

func (w *loggerWriter) Write(p []byte) (int, error) {
	p = bytes.TrimSpace(p)
	w.logFunc(string(p))
	return len(p), nil
}

// New creates stdlib log.Logger that writes to provided logger on Error level
func New(logger log.Logger) *stdlog.Logger {
	l := log.AddCallerSkip(logger, 3)
	return stdlog.New(&loggerWriter{logFunc: l.Error}, "", 0)
}

// NewAt creates stdlib log.Logger that writes to provided logger on specified level
func NewAt(logger log.Logger, lvl log.Level) (*stdlog.Logger, error) {
	l := log.AddCallerSkip(logger, 3)
	logFunc, err := levelToFunc(l, lvl)
	if err != nil {
		return nil, err
	}
	return stdlog.New(&loggerWriter{logFunc: logFunc}, "", 0), nil
}
