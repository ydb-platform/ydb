// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package errors_test

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"testing"

	"github.com/go-faster/errors"
)

func TestErrorFormatter(t *testing.T) {
	var (
		simple   = &wrapped{"simple", nil}
		elephant = &wrapped{
			"can't adumbrate elephant",
			detailed{},
		}
		nonascii = &wrapped{"café", nil}
		newline  = &wrapped{"msg with\nnewline",
			&wrapped{"and another\none", nil}}
		fallback  = &wrapped{"fallback", os.ErrNotExist}
		oldAndNew = &wrapped{"new style", formatError("old style")}
		framed    = &withFrameAndMore{
			frame: errors.Caller(0),
		}
		opaque = &wrapped{"outer",
			errors.Opaque(&wrapped{"mid",
				&wrapped{"inner", nil}})}
	)
	testCases := []struct {
		err    error
		fmt    string
		want   string
		regexp bool
	}{{
		err:  simple,
		fmt:  "%s",
		want: "simple",
	}, {
		err:  elephant,
		fmt:  "%s",
		want: "can't adumbrate elephant: out of peanuts",
	}, {
		err:  &wrapped{"a", &wrapped{"b", &wrapped{"c", nil}}},
		fmt:  "%s",
		want: "a: b: c",
	}, {
		err: simple,
		fmt: "%+v",
		want: "simple:" +
			"\n    somefile.go:123",
	}, {
		err: elephant,
		fmt: "%+v",
		want: "can't adumbrate elephant:" +
			"\n    somefile.go:123" +
			"\n  - out of peanuts:" +
			"\n    the elephant is on strike" +
			"\n    and the 12 monkeys" +
			"\n    are laughing",
	}, {
		err:  &oneNewline{nil},
		fmt:  "%+v",
		want: "123",
	}, {
		err: &oneNewline{&oneNewline{nil}},
		fmt: "%+v",
		want: "123:" +
			"\n  - 123",
	}, {
		err:  &newlineAtEnd{nil},
		fmt:  "%+v",
		want: "newlineAtEnd:\n    detail",
	}, {
		err: &newlineAtEnd{&newlineAtEnd{nil}},
		fmt: "%+v",
		want: "newlineAtEnd:" +
			"\n    detail" +
			"\n  - newlineAtEnd:" +
			"\n    detail",
	}, {
		err: framed,
		fmt: "%+v",
		want: "something:" +
			"\n    github.com/go-faster/errors_test.TestErrorFormatter" +
			"\n        .+/format_test.go:30" +
			"\n    something more",
		regexp: true,
	}, {
		err:  fmtTwice("Hello World!"),
		fmt:  "%#v",
		want: "2 times Hello World!",
	}, {
		err:  fallback,
		fmt:  "%s",
		want: "fallback: file does not exist",
	}, {
		err: fallback,
		fmt: "%+v",
		// Note: no colon after the last error, as there are no details.
		want: "fallback:" +
			"\n    somefile.go:123" +
			"\n  - file does not exist",
	}, {
		err:  opaque,
		fmt:  "%s",
		want: "outer: mid: inner",
	}, {
		err: opaque,
		fmt: "%+v",
		want: "outer:" +
			"\n    somefile.go:123" +
			"\n  - mid:" +
			"\n    somefile.go:123" +
			"\n  - inner:" +
			"\n    somefile.go:123",
	}, {
		err:  oldAndNew,
		fmt:  "%v",
		want: "new style: old style",
	}, {
		err:  oldAndNew,
		fmt:  "%q",
		want: `"new style: old style"`,
	}, {
		err: oldAndNew,
		fmt: "%+v",
		// Note the extra indentation.
		// Colon for old style error is rendered by the fmt.Formatter
		// implementation of the old-style error.
		want: "new style:" +
			"\n    somefile.go:123" +
			"\n  - old style:" +
			"\n    otherfile.go:456",
	}, {
		err:  simple,
		fmt:  "%-12s",
		want: "simple      ",
	}, {
		// Don't use formatting flags for detailed view.
		err: simple,
		fmt: "%+12v",
		want: "simple:" +
			"\n    somefile.go:123",
	}, {
		err:  elephant,
		fmt:  "%+50s",
		want: "          can't adumbrate elephant: out of peanuts",
	}, {
		err:  nonascii,
		fmt:  "%q",
		want: `"café"`,
	}, {
		err:  nonascii,
		fmt:  "%+q",
		want: `"caf\u00e9"`,
	}, {
		err:  simple,
		fmt:  "% x",
		want: "73 69 6d 70 6c 65",
	}, {
		err: newline,
		fmt: "%s",
		want: "msg with" +
			"\nnewline: and another" +
			"\none",
	}, {
		err: newline,
		fmt: "%+v",
		want: "msg with" +
			"\n    newline:" +
			"\n    somefile.go:123" +
			"\n  - and another" +
			"\n    one:" +
			"\n    somefile.go:123",
	}, {
		err: &wrapped{"", &wrapped{"inner message", nil}},
		fmt: "%+v",
		want: "somefile.go:123" +
			"\n  - inner message:" +
			"\n    somefile.go:123",
	}, {
		err:  spurious(""),
		fmt:  "%s",
		want: "spurious",
	}, {
		err:  spurious(""),
		fmt:  "%+v",
		want: "spurious",
	}, {
		err:  spurious("extra"),
		fmt:  "%s",
		want: "spurious",
	}, {
		err: spurious("extra"),
		fmt: "%+v",
		want: "spurious:\n" +
			"    extra",
	}, {
		err:  nil,
		fmt:  "%+v",
		want: "<nil>",
	}, {
		err:  (*wrapped)(nil),
		fmt:  "%+v",
		want: "<nil>",
	}, {
		err:  simple,
		fmt:  "%T",
		want: "*errors_test.wrapped",
	}, {
		err:  simple,
		fmt:  "%🤪",
		want: "%!🤪(*errors_test.wrapped)",
		// For 1.13:
		//  want: "%!🤪(*errors_test.wrapped=&{simple <nil>})",
	}, {
		err:  formatError("use fmt.Formatter"),
		fmt:  "%#v",
		want: "use fmt.Formatter",
	}, {
		err: fmtTwice("%s %s", "ok", panicValue{}),
		fmt: "%s",
		// Different Go versions produce different results.
		want:   `ok %!s\(PANIC=(String method: )?panic\)/ok %!s\(PANIC=(String method: )?panic\)`,
		regexp: true,
	}, {
		err:  fmtTwice("%o %s", panicValue{}, "ok"),
		fmt:  "%s",
		want: "{} ok/{} ok",
	}, {
		err: adapted{"adapted", nil},
		fmt: "%+v",
		want: "adapted:" +
			"\n    detail",
	}, {
		err: adapted{"outer", adapted{"mid", adapted{"inner", nil}}},
		fmt: "%+v",
		want: "outer:" +
			"\n    detail" +
			"\n  - mid:" +
			"\n    detail" +
			"\n  - inner:" +
			"\n    detail",
	}}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s", i, tc.fmt), func(t *testing.T) {
			got := fmt.Sprintf(tc.fmt, tc.err)
			var ok bool
			if tc.regexp {
				var err error
				ok, err = regexp.MatchString(tc.want+"$", got)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				ok = got == tc.want
			}
			if !ok {
				t.Errorf("\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

func TestAdaptor(t *testing.T) {
	testCases := []struct {
		err    error
		fmt    string
		want   string
		regexp bool
	}{{
		err: adapted{"adapted", nil},
		fmt: "%+v",
		want: "adapted:" +
			"\n    detail",
	}, {
		err: adapted{"outer", adapted{"mid", adapted{"inner", nil}}},
		fmt: "%+v",
		want: "outer:" +
			"\n    detail" +
			"\n  - mid:" +
			"\n    detail" +
			"\n  - inner:" +
			"\n    detail",
	}}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s", i, tc.fmt), func(t *testing.T) {
			got := fmt.Sprintf(tc.fmt, tc.err)
			if got != tc.want {
				t.Errorf("\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

var _ errors.Formatter = wrapped{}

type wrapped struct {
	msg string
	err error
}

func (e wrapped) Error() string { return "should call Format" }

func (e wrapped) Format(s fmt.State, verb rune) {
	errors.FormatError(&e, s, verb)
}

func (e wrapped) FormatError(p errors.Printer) (next error) {
	p.Print(e.msg)
	p.Detail()
	p.Print("somefile.go:123")
	return e.err
}

var _ errors.Formatter = detailed{}

type detailed struct{}

func (e detailed) Error() string { panic("should have called FormatError") }

func (detailed) FormatError(p errors.Printer) (next error) {
	p.Printf("out of %s", "peanuts")
	p.Detail()
	p.Print("the elephant is on strike\n")
	p.Printf("and the %d monkeys\nare laughing", 12)
	return nil
}

type withFrameAndMore struct {
	frame errors.Frame
}

func (e *withFrameAndMore) Error() string { return fmt.Sprint(e) }

func (e *withFrameAndMore) Format(s fmt.State, v rune) {
	errors.FormatError(e, s, v)
}

func (e *withFrameAndMore) FormatError(p errors.Printer) (next error) {
	p.Print("something")
	if p.Detail() {
		e.frame.Format(p)
		p.Print("something more")
	}
	return nil
}

type spurious string

func (e spurious) Error() string { return fmt.Sprint(e) }

// move to 1_12 test file
func (e spurious) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e spurious) FormatError(p errors.Printer) (next error) {
	p.Print("spurious")
	p.Detail() // Call detail even if we don't print anything
	if e == "" {
		p.Print()
	} else {
		p.Print("\n", string(e)) // print extraneous leading newline
	}
	return nil
}

type oneNewline struct {
	next error
}

func (e *oneNewline) Error() string { return fmt.Sprint(e) }

func (e *oneNewline) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e *oneNewline) FormatError(p errors.Printer) (next error) {
	p.Print("1")
	p.Print("2")
	p.Print("3")
	p.Detail()
	p.Print("\n")
	return e.next
}

type newlineAtEnd struct {
	next error
}

func (e *newlineAtEnd) Error() string { return fmt.Sprint(e) }

func (e *newlineAtEnd) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e *newlineAtEnd) FormatError(p errors.Printer) (next error) {
	p.Print("newlineAtEnd")
	p.Detail()
	p.Print("detail\n")
	return e.next
}

type adapted struct {
	msg string
	err error
}

func (e adapted) Error() string { return e.msg }

func (e adapted) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e adapted) FormatError(p errors.Printer) error {
	p.Print(e.msg)
	p.Detail()
	p.Print("detail")
	return e.err
}

// formatError is an error implementing Format instead of errors.Formatter.
// The implementation mimics the implementation of github.com/pkg/errors.
type formatError string

func (e formatError) Error() string { return string(e) }

func (e formatError) Format(s fmt.State, verb rune) {
	// Body based on pkg/errors/errors.go
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, string(e))
			fmt.Fprintf(s, ":\n%s", "otherfile.go:456")
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, string(e))
	case 'q':
		fmt.Fprintf(s, "%q", string(e))
	}
}

func (e formatError) GoString() string {
	panic("should never be called")
}

type fmtTwiceErr struct {
	format string
	args   []interface{}
}

func fmtTwice(format string, a ...interface{}) error {
	return fmtTwiceErr{format, a}
}

func (e fmtTwiceErr) Error() string { return fmt.Sprint(e) }

func (e fmtTwiceErr) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e fmtTwiceErr) FormatError(p errors.Printer) (next error) {
	p.Printf(e.format, e.args...)
	p.Print("/")
	p.Printf(e.format, e.args...)
	return nil
}

func (e fmtTwiceErr) GoString() string {
	return "2 times " + fmt.Sprintf(e.format, e.args...)
}

type panicValue struct{}

func (panicValue) String() string { panic("panic") }

func TestFormatError(t *testing.T) {
	if !errors.Is(errors.Errorf("foo: %w", io.EOF), io.EOF) {
		t.Error("fallback expected")
	}
}
