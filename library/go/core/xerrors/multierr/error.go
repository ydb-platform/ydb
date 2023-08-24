package multierr

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

type errorGroup interface {
	Errors() []error
}

// Errors returns a slice containing zero or more errors the supplied
// error is composed of. If the error is nil, a nil slice is returned.
//
// If the error is not composed of the errors (do not implement errorGroup
// interface with Errors method), the returned slice contains just the error
// that was padded in.
//
// Callers of this function are free to modify the returned slice.
func Errors(err error) []error {
	if err == nil {
		return nil
	}

	eg, ok := err.(errorGroup)
	if !ok {
		return []error{err}
	}

	errs := eg.Errors()

	result := make([]error, len(errs))
	copy(result, errs)

	return result
}

// Append appends the given errors together. Either value may be nil.
//
// This function is a specialization of Combine for the common case where
// there are only two errors.
func Append(left, right error) error {
	switch {
	case left == nil:
		return right
	case right == nil:
		return left
	}

	if _, ok := right.(*multiError); !ok {
		if l, ok := left.(*multiError); ok && atomic.SwapUint32(&l.copyNeeded, 1) == 0 {
			errors := append(l.Errors(), right)

			return &multiError{errors: errors}
		} else if !ok {
			return &multiError{errors: []error{left, right}}
		}
	}

	return fromSlice([]error{left, right})
}

// Combine combines the passed errors into a single error.
//
// If zero arguments were passed or if all items are nil,
// a nil error is returned.
//
// If only a single error was passed, it is returned as-is.
//
// Combine skips over nil arguments so this function may be
// used to combine errors from operations that fail independently
// of each other.
//
// If any of the passed errors is an errorGroup error, it will be
// flattened along with the other errors.
func Combine(errors ...error) error {
	return fromSlice(errors)
}

func fromSlice(errors []error) error {
	inspection := inspect(errors)

	switch inspection.topLevelErrorsCount {
	case 0:
		return nil
	case 1:
		return errors[inspection.firstErrorIdx]
	case len(errors):
		if !inspection.containsErrorGroup {
			return &multiError{errors: errors}
		}
	}

	nonNilErrs := make([]error, 0, inspection.errorsCapacity)

	for _, err := range errors[inspection.firstErrorIdx:] {
		if err == nil {
			continue
		}

		if eg, ok := err.(errorGroup); ok {
			nonNilErrs = append(nonNilErrs, eg.Errors()...)

			continue
		}

		nonNilErrs = append(nonNilErrs, err)
	}

	return &multiError{errors: nonNilErrs}
}

type errorsInspection struct {
	topLevelErrorsCount int
	errorsCapacity      int
	firstErrorIdx       int
	containsErrorGroup  bool
}

func inspect(errors []error) errorsInspection {
	var inspection errorsInspection

	first := true

	for i, err := range errors {
		if err == nil {
			continue
		}

		inspection.topLevelErrorsCount++
		if first {
			first = false
			inspection.firstErrorIdx = i
		}

		if eg, ok := err.(errorGroup); ok {
			inspection.containsErrorGroup = true
			inspection.errorsCapacity += len(eg.Errors())

			continue
		}

		inspection.errorsCapacity++
	}

	return inspection
}

type multiError struct {
	copyNeeded uint32
	errors     []error
}

// As attempts to find the first error in the error list
// that matched the type of the value that target points to.
//
// This function allows errors.As to traverse the values stored on the
// multiError error.
func (e *multiError) As(target interface{}) bool {
	for _, err := range e.Errors() {
		if xerrors.As(err, target) {
			return true
		}
	}

	return false
}

// Is attempts to match the provided error against
// errors in the error list.
//
// This function allows errors.Is to traverse the values stored on the
// multiError error.
func (e *multiError) Is(target error) bool {
	for _, err := range e.Errors() {
		if xerrors.Is(err, target) {
			return true
		}
	}

	return false
}

func (e *multiError) Error() string {
	if e == nil {
		return ""
	}

	var buff bytes.Buffer

	e.writeSingleLine(&buff)

	return buff.String()
}

// Errors returns the list of underlying errors.
//
// This slice MUST NOT be modified.
func (e *multiError) Errors() []error {
	if e == nil {
		return nil
	}

	return e.errors
}

var (
	singleLineSeparator = []byte("; ")

	multiLineSeparator = []byte("\n")
	multiLineIndent    = []byte("  ")
)

func (e *multiError) writeSingleLine(w io.Writer) {
	first := true

	for _, err := range e.Errors() {
		if first {
			first = false
		} else {
			_, _ = w.Write(singleLineSeparator)
		}

		_, _ = io.WriteString(w, err.Error())
	}
}

func (e *multiError) Format(f fmt.State, c rune) {
	if c == 'v' && f.Flag('+') {
		e.writeMultiLine(f)

		return
	}

	e.writeSingleLine(f)
}

func (e *multiError) writeMultiLine(w io.Writer) {
	var (
		errors  = e.Errors()
		lastIdx = len(errors) - 1
	)

	for _, err := range errors[:lastIdx] {
		writePrefixLine(w, multiLineIndent, fmt.Sprintf("%+v", err))

		_, _ = w.Write(multiLineSeparator)
	}

	writePrefixLine(w, multiLineIndent, fmt.Sprintf("%+v", errors[lastIdx]))
}

func writePrefixLine(w io.Writer, prefix []byte, s string) {
	first := true

	for len(s) > 0 {
		if first {
			first = false
		} else {
			_, _ = w.Write(prefix)
		}

		idx := strings.IndexByte(s, '\n')
		if idx < 0 {
			idx = len(s) - 1
		}

		_, _ = io.WriteString(w, s[:idx+1])

		s = s[idx+1:]
	}
}
