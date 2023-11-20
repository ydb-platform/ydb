// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errors_test

import (
	"fmt"

	"github.com/go-faster/errors"
)

type MyError2 struct {
	Message string
	frame   errors.Frame
}

func (m *MyError2) Error() string {
	return m.Message
}

func (m *MyError2) Format(f fmt.State, c rune) { // implements fmt.Formatter
	errors.FormatError(m, f, c)
}

func (m *MyError2) FormatError(p errors.Printer) error { // implements errors.Formatter
	p.Print(m.Message)
	if p.Detail() {
		m.frame.Format(p)
	}
	return nil
}

func ExampleFormatError() {
	err := &MyError2{Message: "oops", frame: errors.Caller(1)}
	fmt.Printf("%v\n", err)
	fmt.Println()
	fmt.Printf("%+v\n", err)
}
