// Package cgosem implements fast and imprecise semaphore used to globally limit concurrency of _fast_ cgo calls.
//
// In the future, when go runtime scheduler gets smarter and stop suffering from uncontrolled growth the number of
// system threads, this package should be removed.
//
// See "Cgoroutines != Goroutines" section of https://www.cockroachlabs.com/blog/the-cost-and-complexity-of-cgo/
// for explanation of the thread leak problem.
//
// To use this semaphore, put the following line at the beginning of the function doing Cgo calls.
//
//	defer cgosem.S.Acquire().Release()
//
// This will globally limit number of concurrent Cgo calls to GOMAXPROCS, limiting number of additional threads created by the
// go runtime to the same number.
//
// Overhead of this semaphore is about 1us, which should be negligible compared to the work you are trying to do in the C function.
//
// To see code in action, run:
//
//	ya make -r library/go/cgosem/gotest
//	env GODEBUG=schedtrace=1000,scheddetail=1 library/go/cgosem/gotest/gotest --test.run TestLeak
//	env GODEBUG=schedtrace=1000,scheddetail=1 library/go/cgosem/gotest/gotest --test.run TestLeakFix
//
// And look for the number of created M's.
package cgosem

import "runtime"

type Sem chan struct{}

// new creates new semaphore with max concurrency of n.
func newSem(n int) (s Sem) {
	s = make(chan struct{}, n)
	for i := 0; i < n; i++ {
		s <- struct{}{}
	}
	return
}

func (s Sem) Acquire() Sem {
	if s == nil {
		return nil
	}

	<-s
	return s
}

func (s Sem) Release() {
	if s == nil {
		return
	}

	s <- struct{}{}
}

// S is global semaphore with good enough settings for most cgo libraries.
var S Sem

// Disable global cgo semaphore. Must be called from init() function.
func Disable() {
	S = nil
}

func init() {
	S = newSem(runtime.GOMAXPROCS(0))
}
