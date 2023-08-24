package logrus

import (
	"io"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/ydb-platform/ydb/library/go/core/log"
)

/* Call frame calculations are copied from logrus package */
var (

	// qualified package name, cached at first use
	logrusPackage string

	// Positions in the call stack when tracing to report the calling method
	minimumCallerDepth int

	// Used for caller information initialisation
	callerInitOnce sync.Once
)

const (
	maximumCallerDepth int = 25
	knownLogrusFrames  int = 4
)

func init() {
	// start at the bottom of the stack before the package-name cache is primed
	minimumCallerDepth = 1
}

// getPackageName reduces a fully qualified function name to the package name
// There really ought to be to be a better way...
func getPackageName(f string) string {
	for {
		lastPeriod := strings.LastIndex(f, ".")
		lastSlash := strings.LastIndex(f, "/")
		if lastPeriod > lastSlash {
			f = f[:lastPeriod]
		} else {
			break
		}
	}

	return f
}

func getCallerDepth() int {
	// cache this package's fully-qualified name
	callerInitOnce.Do(func() {
		pcs := make([]uintptr, maximumCallerDepth)
		_ = runtime.Callers(0, pcs)

		// dynamic get the package name and the minimum caller depth
		logrusIsNext := false
		for i := 0; i < maximumCallerDepth; i++ {
			funcName := runtime.FuncForPC(pcs[i]).Name()
			if logrusIsNext {
				logrusPackage = getPackageName(funcName)
				break
			}
			if strings.Contains(funcName, "LogrusAdapter") {
				logrusIsNext = true
				continue
			}
		}

		minimumCallerDepth = knownLogrusFrames
	})

	// Restrict the lookback frames to avoid runaway lookups
	pcs := make([]uintptr, maximumCallerDepth)
	depth := runtime.Callers(minimumCallerDepth, pcs)
	frames := runtime.CallersFrames(pcs[:depth])
	callerDepth := minimumCallerDepth

	for f, again := frames.Next(); again; f, again = frames.Next() {
		pkg := getPackageName(f.Function)

		// If the caller isn't part of this package, we're done
		if pkg != logrusPackage {
			return callerDepth - 2
		}
		callerDepth++
	}

	// if we got here, we failed to find the caller's context
	return 0
}

func convertLevel(level log.Level) logrus.Level {
	switch level {
	case log.TraceLevel:
		return logrus.TraceLevel
	case log.DebugLevel:
		return logrus.DebugLevel
	case log.InfoLevel:
		return logrus.InfoLevel
	case log.WarnLevel:
		return logrus.WarnLevel
	case log.ErrorLevel:
		return logrus.ErrorLevel
	case log.FatalLevel:
		return logrus.FatalLevel
	}

	return logrus.PanicLevel
}

func SetLevel(level log.Level) {
	logrus.SetLevel(convertLevel(level))
}

type LogrusAdapter struct {
	logger         log.Logger
	adaptCallstack bool
	convertPrefix  bool
}

func (a *LogrusAdapter) Format(entry *logrus.Entry) ([]byte, error) {
	var name *string
	fields := make([]log.Field, 0, len(entry.Data))
	for key, val := range entry.Data {
		skip := false
		if a.convertPrefix && key == "prefix" {
			if w, ok := val.(string); ok {
				name = &w
				skip = true
			}
		}
		if !skip {
			fields = append(fields, log.Any(key, val))
		}
	}

	var logger log.Logger
	if a.adaptCallstack {
		logger = log.AddCallerSkip(a.logger, getCallerDepth())
	} else {
		logger = a.logger
	}

	if a.convertPrefix && name != nil {
		logger = logger.WithName(*name)
	}

	switch entry.Level {
	case logrus.TraceLevel:
		logger.Trace(entry.Message, fields...)
	case logrus.DebugLevel:
		logger.Debug(entry.Message, fields...)
	case logrus.InfoLevel:
		logger.Info(entry.Message, fields...)
	case logrus.WarnLevel:
		logger.Warn(entry.Message, fields...)
	case logrus.ErrorLevel:
		logger.Error(entry.Message, fields...)
	case logrus.FatalLevel:
		logger.Fatal(entry.Message, fields...)
	case logrus.PanicLevel:
		logger.Fatal(entry.Message, fields...)
	}

	return nil, nil
}

type Option func(*LogrusAdapter)

func DontAdaptCallstack() Option {
	return func(adapter *LogrusAdapter) {
		adapter.adaptCallstack = false
	}
}

func ConvertPrefix() Option {
	return func(adapter *LogrusAdapter) {
		adapter.convertPrefix = true
	}
}

// AdaptLogrus replaces logr formatter by wrapped logger
func AdaptLogrus(logr *logrus.Logger, logger log.Logger, level log.Level, opts ...Option) {
	logr.SetLevel(convertLevel(level))

	adapter := &LogrusAdapter{logger, true, false}

	for _, opt := range opts {
		opt(adapter)
	}

	logr.SetFormatter(adapter)
	logr.SetOutput(io.Discard)
}

// AdaptStandardLogger replaces logrus.StandardLogger() formatter by wrapped logger
func AdaptStandardLogger(logger log.Logger, level log.Level, opts ...Option) {
	AdaptLogrus(logrus.StandardLogger(), logger, level, opts...)
}
