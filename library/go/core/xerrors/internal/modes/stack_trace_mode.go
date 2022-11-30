package modes

import "sync/atomic"

type StackTraceMode int32

const (
	StackTraceModeFrames StackTraceMode = iota
	StackTraceModeStacks
	StackTraceModeStackThenFrames
	StackTraceModeStackThenNothing
	StackTraceModeNothing
)

func (m StackTraceMode) String() string {
	return []string{"Frames", "Stacks", "StackThenFrames", "StackThenNothing", "Nothing"}[m]
}

const defaultStackTraceMode = StackTraceModeFrames

var (
	// Default mode
	stackTraceMode = defaultStackTraceMode
	// Known modes (used in tests)
	knownStackTraceModes = []StackTraceMode{
		StackTraceModeFrames,
		StackTraceModeStacks,
		StackTraceModeStackThenFrames,
		StackTraceModeStackThenNothing,
		StackTraceModeNothing,
	}
)

func SetStackTraceMode(v StackTraceMode) {
	atomic.StoreInt32((*int32)(&stackTraceMode), int32(v))
}

func GetStackTraceMode() StackTraceMode {
	return StackTraceMode(atomic.LoadInt32((*int32)(&stackTraceMode)))
}

func DefaultStackTraceMode() {
	SetStackTraceMode(defaultStackTraceMode)
}

func KnownStackTraceModes() []StackTraceMode {
	return knownStackTraceModes
}
