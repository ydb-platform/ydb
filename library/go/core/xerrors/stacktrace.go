package xerrors

import (
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb/library/go/x/xruntime"
)

func writeStackTrace(w io.Writer, stacktrace *xruntime.StackTrace) {
	for _, frame := range stacktrace.Frames() {
		if frame.Function != "" {
			_, _ = fmt.Fprintf(w, "    %s\n    ", frame.Function)
		}

		if frame.File != "" {
			_, _ = fmt.Fprintf(w, "    %s:%d\n", frame.File, frame.Line)
		}
	}
}

type ErrorStackTrace interface {
	StackTrace() *xruntime.StackTrace
}

// StackTraceOfEffect returns last stacktrace that was added to error chain (furthest from the root error).
// Guarantees that returned value has valid StackTrace object (but not that there are any frames).
func StackTraceOfEffect(err error) ErrorStackTrace {
	var st ErrorStackTrace
	for {
		if !As(err, &st) {
			return nil
		}

		if st.StackTrace() != nil {
			return st
		}

		err = st.(error)
		err = errors.Unwrap(err)
	}
}

// StackTraceOfCause returns first stacktrace that was added to error chain (closest to the root error).
// Guarantees that returned value has valid StackTrace object (but not that there are any frames).
func StackTraceOfCause(err error) ErrorStackTrace {
	var res ErrorStackTrace
	var st ErrorStackTrace
	for {
		if !As(err, &st) {
			return res
		}

		if st.StackTrace() != nil {
			res = st
		}

		err = st.(error)
		err = errors.Unwrap(err)
	}
}

// NextStackTracer returns next error with stack trace.
// Guarantees that returned value has valid StackTrace object (but not that there are any frames).
func NextStackTrace(st ErrorStackTrace) ErrorStackTrace {
	var res ErrorStackTrace
	for {
		err := st.(error)
		err = errors.Unwrap(err)

		if !As(err, &res) {
			return nil
		}

		if res.StackTrace() != nil {
			return res
		}
	}
}
