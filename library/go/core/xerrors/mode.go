package xerrors

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/xerrors/internal/modes"
	"github.com/ydb-platform/ydb/library/go/x/xruntime"
)

func DefaultStackTraceMode() {
	modes.DefaultStackTraceMode()
}

func EnableFrames() {
	modes.SetStackTraceMode(modes.StackTraceModeFrames)
}

func EnableStacks() {
	modes.SetStackTraceMode(modes.StackTraceModeStacks)
}

func EnableStackThenFrames() {
	modes.SetStackTraceMode(modes.StackTraceModeStackThenFrames)
}

func EnableStackThenNothing() {
	modes.SetStackTraceMode(modes.StackTraceModeStackThenNothing)
}

func DisableStackTraces() {
	modes.SetStackTraceMode(modes.StackTraceModeNothing)
}

// newStackTrace returns stacktrace based on current mode and frames count
func newStackTrace(skip int, err error) *xruntime.StackTrace {
	skip++
	m := modes.GetStackTraceMode()
	switch m {
	case modes.StackTraceModeFrames:
		return xruntime.NewFrame(skip)
	case modes.StackTraceModeStackThenFrames:
		if err != nil && StackTraceOfEffect(err) != nil {
			return xruntime.NewFrame(skip)
		}

		return _newStackTrace(skip)
	case modes.StackTraceModeStackThenNothing:
		if err != nil && StackTraceOfEffect(err) != nil {
			return nil
		}

		return _newStackTrace(skip)
	case modes.StackTraceModeStacks:
		return _newStackTrace(skip)
	case modes.StackTraceModeNothing:
		return nil
	}

	panic(fmt.Sprintf("unknown stack trace mode %d", m))
}

func MaxStackFrames16() {
	modes.SetStackFramesCountMax(modes.StackFramesCount16)
}

func MaxStackFrames32() {
	modes.SetStackFramesCountMax(modes.StackFramesCount32)
}

func MaxStackFrames64() {
	modes.SetStackFramesCountMax(modes.StackFramesCount64)
}

func MaxStackFrames128() {
	modes.SetStackFramesCountMax(modes.StackFramesCount128)
}

func _newStackTrace(skip int) *xruntime.StackTrace {
	skip++
	count := modes.GetStackFramesCountMax()
	switch count {
	case 16:
		return xruntime.NewStackTrace16(skip)
	case 32:
		return xruntime.NewStackTrace32(skip)
	case 64:
		return xruntime.NewStackTrace64(skip)
	case 128:
		return xruntime.NewStackTrace128(skip)
	}

	panic(fmt.Sprintf("unknown stack frames count %d", count))
}
