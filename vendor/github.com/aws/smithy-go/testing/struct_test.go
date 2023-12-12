package testing

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func TestCompareStructEqual(t *testing.T) {
	cases := map[string]struct {
		A, B      interface{}
		ExpectErr string
	}{
		"simple match": {
			A: struct {
				Foo string
				Bar int
			}{
				Foo: "abc",
				Bar: 123,
			},
			B: struct {
				Foo string
				Bar int
			}{
				Foo: "abc",
				Bar: 123,
			},
		},
		"simple diff": {
			A: struct {
				Foo string
				Bar int
			}{
				Foo: "abc",
				Bar: 123,
			},
			B: struct {
				Foo string
				Bar int
			}{
				Foo: "abc",
				Bar: 456,
			},
			ExpectErr: "values do not match",
		},
		"reader match": {
			A: struct {
				Foo io.Reader
				Bar int
			}{
				Foo: bytes.NewBuffer([]byte("abc123")),
				Bar: 123,
			},
			B: struct {
				Foo io.Reader
				Bar int
			}{
				Foo: ioutil.NopCloser(strings.NewReader("abc123")),
				Bar: 123,
			},
		},
		"reader diff": {
			A: struct {
				Foo io.Reader
				Bar int
			}{
				Foo: bytes.NewBuffer([]byte("abc123")),
				Bar: 123,
			},
			B: struct {
				Foo io.Reader
				Bar int
			}{
				Foo: ioutil.NopCloser(strings.NewReader("123abc")),
				Bar: 123,
			},
			ExpectErr: "bytes do not match",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := CompareValues(c.A, c.B)

			if len(c.ExpectErr) != 0 {
				if err == nil {
					t.Fatalf("expect error, got none")
				}
				if e, a := c.ExpectErr, err.Error(); !strings.Contains(a, e) {
					t.Fatalf("expect error to contain %v, got %v", e, a)
				}
				return
			}
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}
		})
	}
}
