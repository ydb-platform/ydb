package xruntime

import (
	"runtime"
)

type StackTrace struct {
	frames []uintptr
	full   bool
}

func NewStackTrace16(skip int) *StackTrace {
	var pcs [16]uintptr
	return newStackTrace(skip+2, pcs[:])
}

func NewStackTrace32(skip int) *StackTrace {
	var pcs [32]uintptr
	return newStackTrace(skip+2, pcs[:])
}

func NewStackTrace64(skip int) *StackTrace {
	var pcs [64]uintptr
	return newStackTrace(skip+2, pcs[:])
}

func NewStackTrace128(skip int) *StackTrace {
	var pcs [128]uintptr
	return newStackTrace(skip+2, pcs[:])
}

func newStackTrace(skip int, pcs []uintptr) *StackTrace {
	n := runtime.Callers(skip+1, pcs)
	return &StackTrace{frames: pcs[:n], full: true}
}

func NewFrame(skip int) *StackTrace {
	var pcs [3]uintptr
	n := runtime.Callers(skip+1, pcs[:])
	return &StackTrace{frames: pcs[:n]}
}

func (st *StackTrace) Frames() []runtime.Frame {
	frames := runtime.CallersFrames(st.frames[:])
	if !st.full {
		if _, ok := frames.Next(); !ok {
			return nil
		}

		fr, ok := frames.Next()
		if !ok {
			return nil
		}

		return []runtime.Frame{fr}
	}

	var res []runtime.Frame
	for {
		frame, more := frames.Next()
		if !more {
			break
		}

		res = append(res, frame)
	}

	return res
}
