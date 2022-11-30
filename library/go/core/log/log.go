package log

import "errors"

// Logger is the universal logger that can do everything.
type Logger interface {
	loggerStructured
	loggerFmt
	toStructured
	toFmt
	withName
}

type withName interface {
	WithName(name string) Logger
}

type toLogger interface {
	// Logger returns general logger
	Logger() Logger
}

// Structured provides interface for logging using fields.
type Structured interface {
	loggerStructured
	toFmt
	toLogger
}

type loggerStructured interface {
	// Trace logs at Trace log level using fields
	Trace(msg string, fields ...Field)
	// Debug logs at Debug log level using fields
	Debug(msg string, fields ...Field)
	// Info logs at Info log level using fields
	Info(msg string, fields ...Field)
	// Warn logs at Warn log level using fields
	Warn(msg string, fields ...Field)
	// Error logs at Error log level using fields
	Error(msg string, fields ...Field)
	// Fatal logs at Fatal log level using fields
	Fatal(msg string, fields ...Field)
}

type toFmt interface {
	// Fmt returns fmt logger
	Fmt() Fmt
}

// Fmt provides interface for logging using fmt formatter.
type Fmt interface {
	loggerFmt
	toStructured
	toLogger
}

type loggerFmt interface {
	// Tracef logs at Trace log level using fmt formatter
	Tracef(format string, args ...interface{})
	// Debugf logs at Debug log level using fmt formatter
	Debugf(format string, args ...interface{})
	// Infof logs at Info log level using fmt formatter
	Infof(format string, args ...interface{})
	// Warnf logs at Warn log level using fmt formatter
	Warnf(format string, args ...interface{})
	// Errorf logs at Error log level using fmt formatter
	Errorf(format string, args ...interface{})
	// Fatalf logs at Fatal log level using fmt formatter
	Fatalf(format string, args ...interface{})
}

type toStructured interface {
	// Structured returns structured logger
	Structured() Structured
}

// LoggerWith is an interface for 'With' function
// LoggerWith provides interface for logger modifications.
type LoggerWith interface {
	// With implements 'With'
	With(fields ...Field) Logger
}

// With for loggers that implement LoggerWith interface, returns logger that
// always adds provided key/value to every log entry. Otherwise returns same logger.
func With(l Logger, fields ...Field) Logger {
	e, ok := l.(LoggerWith)
	if !ok {
		return l
	}

	return e.With(fields...)
}

// LoggerAddCallerSkip is an interface for 'AddCallerSkip' function
type LoggerAddCallerSkip interface {
	// AddCallerSkip implements 'AddCallerSkip'
	AddCallerSkip(skip int) Logger
}

// AddCallerSkip for loggers that implement LoggerAddCallerSkip interface, returns logger that
// adds caller skip to each log entry. Otherwise returns same logger.
func AddCallerSkip(l Logger, skip int) Logger {
	e, ok := l.(LoggerAddCallerSkip)
	if !ok {
		return l
	}

	return e.AddCallerSkip(skip)
}

// WriteAt is a helper method that checks logger and writes message at given level
func WriteAt(l Structured, lvl Level, msg string, fields ...Field) error {
	if l == nil {
		return errors.New("nil logger given")
	}

	switch lvl {
	case DebugLevel:
		l.Debug(msg, fields...)
	case TraceLevel:
		l.Trace(msg, fields...)
	case InfoLevel:
		l.Info(msg, fields...)
	case WarnLevel:
		l.Warn(msg, fields...)
	case ErrorLevel:
		l.Error(msg, fields...)
	case FatalLevel:
		l.Fatal(msg, fields...)
	}

	return nil
}
