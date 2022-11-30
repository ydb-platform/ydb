//go:build cgo
// +build cgo

package tvmauth

import "C"
import (
	"fmt"
	"sync"

	"a.yandex-team.ru/library/go/core/log"
)

// CGO pointer rules state:
//
// Go code may pass a Go pointer to C provided the Go memory to which it points **does not contain any Go pointers**.
//
// Logger is an interface and contains pointer to implementation. That means, we are forbidden from
// passing Logger to C code.
//
// Instead, we put logger into a global map and pass key to the C code.
//
// This might seem inefficient, but we are not concerned with performance here, since the logger is not on the hot path anyway.

var (
	loggersLock sync.Mutex
	nextSlot    int
	loggers     = map[int]log.Logger{}
)

func registerLogger(l log.Logger) int {
	loggersLock.Lock()
	defer loggersLock.Unlock()

	i := nextSlot
	nextSlot++
	loggers[i] = l
	return i
}

func unregisterLogger(i int) {
	loggersLock.Lock()
	defer loggersLock.Unlock()

	if _, ok := loggers[i]; !ok {
		panic(fmt.Sprintf("attempt to unregister unknown logger %d", i))
	}

	delete(loggers, i)
}

func findLogger(i int) log.Logger {
	loggersLock.Lock()
	defer loggersLock.Unlock()

	return loggers[i]
}

//export TVM_WriteToLog
//
// TVM_WriteToLog is technical artifact
func TVM_WriteToLog(logger int, level int, msgData *C.char, msgSize C.int) {
	l := findLogger(logger)

	msg := C.GoStringN(msgData, msgSize)

	switch level {
	case 3:
		l.Error(msg)
	case 4:
		l.Warn(msg)
	case 6:
		l.Info(msg)
	default:
		l.Debug(msg)
	}
}
