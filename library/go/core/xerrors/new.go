package xerrors

import (
	"fmt"
	"io"

	"github.com/ydb-platform/ydb/library/go/x/xruntime"
)

type newError struct {
	msg        string
	stacktrace *xruntime.StackTrace
}

var _ ErrorStackTrace = &newError{}

func New(text string) error {
	return &newError{
		msg:        text,
		stacktrace: newStackTrace(1, nil),
	}
}

func (e *newError) Error() string {
	return e.msg
}

func (e *newError) Format(s fmt.State, v rune) {
	switch v {
	case 'v':
		if s.Flag('+') && e.stacktrace != nil {
			_, _ = io.WriteString(s, e.msg)
			_, _ = io.WriteString(s, "\n")
			writeStackTrace(s, e.stacktrace)
			return
		}

		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.msg)
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.msg)
	}
}

func (e *newError) StackTrace() *xruntime.StackTrace {
	return e.stacktrace
}
