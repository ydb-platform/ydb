package modes

import "sync/atomic"

type StackFramesCount = int32

const (
	StackFramesCount16  StackFramesCount = 16
	StackFramesCount32  StackFramesCount = 32
	StackFramesCount64  StackFramesCount = 64
	StackFramesCount128 StackFramesCount = 128
)

var StackFramesCountMax = StackFramesCount32

func SetStackFramesCountMax(count StackFramesCount) {
	atomic.StoreInt32(&StackFramesCountMax, count)
}

func GetStackFramesCountMax() StackFramesCount {
	return atomic.LoadInt32(&StackFramesCountMax)
}
