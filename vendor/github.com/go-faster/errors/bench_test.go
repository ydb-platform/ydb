// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errors

import (
	"fmt"
	"testing"
)

func BenchmarkWrap(b *testing.B) {
	err := New("foo")
	args := func(a ...interface{}) []interface{} { return a }
	benchCases := []struct {
		name   string
		format string
		args   []interface{}
		msg    string
		err    error
	}{
		{"wrap", "msg: %w", args(err), "wrap", err},
	}
	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			b.Run("WrapTrace", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = Wrap(bc.err, bc.msg)
				}
			})
			b.Run("WrapNoTrace", func(b *testing.B) {
				b.ReportAllocs()
				DisableTrace()
				defer enableTrace()

				for i := 0; i < b.N; i++ {
					_ = Wrap(bc.err, bc.msg)
				}
			})
			b.Run("Core", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = fmt.Errorf(bc.format, bc.args...)
				}
			})
		})
	}
}
