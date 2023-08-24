package xerrors

import (
	"fmt"
	"io"
	"strings"

	"github.com/ydb-platform/ydb/library/go/x/xruntime"
)

type wrappedErrorf struct {
	err        error
	stacktrace *xruntime.StackTrace
}

var _ ErrorStackTrace = &wrappedErrorf{}

func Errorf(format string, a ...interface{}) error {
	err := fmt.Errorf(format, a...)
	return &wrappedErrorf{
		err:        err,
		stacktrace: newStackTrace(1, err),
	}
}

func SkipErrorf(skip int, format string, a ...interface{}) error {
	err := fmt.Errorf(format, a...)
	return &wrappedErrorf{
		err:        err,
		stacktrace: newStackTrace(skip+1, err),
	}
}

func (e *wrappedErrorf) Format(s fmt.State, v rune) {
	switch v {
	case 'v':
		if s.Flag('+') {
			msg := e.err.Error()
			inner := Unwrap(e.err)
			// If Errorf wrapped another error then it will be our message' suffix. If so, cut it since otherwise we will
			// print it again as part of formatting that error.
			if inner != nil {
				if strings.HasSuffix(msg, inner.Error()) {
					msg = msg[:len(msg)-len(inner.Error())]
					// Cut last space if needed but only if there is stacktrace present (very likely)
					if e.stacktrace != nil && strings.HasSuffix(msg, ": ") {
						msg = msg[:len(msg)-1]
					}
				}
			}

			_, _ = io.WriteString(s, msg)
			if e.stacktrace != nil {
				// New line is useful only when printing frames, otherwise it is better to print next error in the chain
				// right after we print this one
				_, _ = io.WriteString(s, "\n")
				writeStackTrace(s, e.stacktrace)
			}

			// Print next error down the chain if there is one
			if inner != nil {
				_, _ = fmt.Fprintf(s, "%+v", inner)
			}

			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.err.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.err.Error())
	}
}

func (e *wrappedErrorf) Error() string {
	// Wrapped error has correct formatting
	return e.err.Error()
}

func (e *wrappedErrorf) Unwrap() error {
	// Skip wrapped error and return whatever it is wrapping if inner error contains single error
	// TODO: test for correct unwrap
	if _, ok := e.err.(interface{ Unwrap() []error }); ok {
		return e.err
	}

	return Unwrap(e.err)
}

func (e *wrappedErrorf) StackTrace() *xruntime.StackTrace {
	return e.stacktrace
}
