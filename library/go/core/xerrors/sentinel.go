package xerrors

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ydb-platform/ydb/library/go/x/xreflect"
	"github.com/ydb-platform/ydb/library/go/x/xruntime"
)

// NewSentinel acts as New but does not add stack frame
func NewSentinel(text string) *Sentinel {
	return &Sentinel{error: errors.New(text)}
}

// Sentinel error
type Sentinel struct {
	error
}

// WithFrame adds stack frame to sentinel error (DEPRECATED)
func (s *Sentinel) WithFrame() error {
	return &sentinelWithStackTrace{
		err:        s,
		stacktrace: newStackTrace(1, nil),
	}
}

func (s *Sentinel) WithStackTrace() error {
	return &sentinelWithStackTrace{
		err:        s,
		stacktrace: newStackTrace(1, nil),
	}
}

// Wrap error with this sentinel error. Adds stack frame.
func (s *Sentinel) Wrap(err error) error {
	if err == nil {
		panic("tried to wrap a nil error")
	}

	return &sentinelWrapper{
		err:        s,
		wrapped:    err,
		stacktrace: newStackTrace(1, err),
	}
}

type sentinelWithStackTrace struct {
	err        error
	stacktrace *xruntime.StackTrace
}

func (e *sentinelWithStackTrace) Error() string {
	return e.err.Error()
}

func (e *sentinelWithStackTrace) Format(s fmt.State, v rune) {
	switch v {
	case 'v':
		if s.Flag('+') && e.stacktrace != nil {
			msg := e.err.Error()
			_, _ = io.WriteString(s, msg)
			writeMsgAndStackTraceSeparator(s, msg)
			writeStackTrace(s, e.stacktrace)
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.err.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.err.Error())
	}
}

func writeMsgAndStackTraceSeparator(w io.Writer, msg string) {
	separator := "\n"
	if !strings.HasSuffix(msg, ":") {
		separator = ":\n"
	}

	_, _ = io.WriteString(w, separator)
}

// Is checks if e holds the specified error. Checks only immediate error.
func (e *sentinelWithStackTrace) Is(target error) bool {
	return e.err == target
}

// As checks if ew holds the specified error type. Checks only immediate error.
// It does NOT perform target checks as it relies on errors.As to do it
func (e *sentinelWithStackTrace) As(target interface{}) bool {
	return xreflect.Assign(e.err, target)
}

type sentinelWrapper struct {
	err        error
	wrapped    error
	stacktrace *xruntime.StackTrace
}

func (e *sentinelWrapper) Error() string {
	return fmt.Sprintf("%s", e)
}

func (e *sentinelWrapper) Format(s fmt.State, v rune) {
	switch v {
	case 'v':
		if s.Flag('+') {
			if e.stacktrace != nil {
				msg := e.err.Error()
				_, _ = io.WriteString(s, msg)
				writeMsgAndStackTraceSeparator(s, msg)
				writeStackTrace(s, e.stacktrace)
				_, _ = fmt.Fprintf(s, "%+v", e.wrapped)
			} else {
				_, _ = io.WriteString(s, e.err.Error())
				_, _ = io.WriteString(s, ": ")
				_, _ = fmt.Fprintf(s, "%+v", e.wrapped)
			}

			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.err.Error())
		_, _ = io.WriteString(s, ": ")
		_, _ = io.WriteString(s, e.wrapped.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", fmt.Sprintf("%s: %s", e.err.Error(), e.wrapped.Error()))
	}
}

// Unwrap implements Wrapper interface
func (e *sentinelWrapper) Unwrap() error {
	return e.wrapped
}

// Is checks if ew holds the specified error. Checks only immediate error.
func (e *sentinelWrapper) Is(target error) bool {
	return e.err == target
}

// As checks if error holds the specified error type. Checks only immediate error.
// It does NOT perform target checks as it relies on errors.As to do it
func (e *sentinelWrapper) As(target interface{}) bool {
	return xreflect.Assign(e.err, target)
}
