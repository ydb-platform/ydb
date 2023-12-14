package httpbinding

import (
	"bytes"
	"testing"
)

func TestPathReplace(t *testing.T) {
	cases := []struct {
		Orig, ExpPath, ExpRawPath []byte
		Key, Val                  string
	}{
		{
			Orig:       []byte("/{bucket}/{key+}"),
			ExpPath:    []byte("/123/{key+}"),
			ExpRawPath: []byte("/123/{key+}"),
			Key:        "bucket", Val: "123",
		},
		{
			Orig:       []byte("/{bucket}/{key+}"),
			ExpPath:    []byte("/{bucket}/abc"),
			ExpRawPath: []byte("/{bucket}/abc"),
			Key:        "key", Val: "abc",
		},
		{
			Orig:       []byte("/{bucket}/{key+}"),
			ExpPath:    []byte("/{bucket}/a/b/c"),
			ExpRawPath: []byte("/{bucket}/a/b/c"),
			Key:        "key", Val: "a/b/c",
		},
		{
			Orig:       []byte("/{bucket}/{key+}"),
			ExpPath:    []byte("/1/2/3/{key+}"),
			ExpRawPath: []byte("/1%2F2%2F3/{key+}"),
			Key:        "bucket", Val: "1/2/3",
		},
		{
			Orig:       []byte("/{bucket}/{key+}"),
			ExpPath:    []byte("/reallylongvaluegoesheregrowingarray/{key+}"),
			ExpRawPath: []byte("/reallylongvaluegoesheregrowingarray/{key+}"),
			Key:        "bucket", Val: "reallylongvaluegoesheregrowingarray",
		},
	}

	var buffer [64]byte

	for i, c := range cases {
		origRaw := make([]byte, len(c.Orig))
		copy(origRaw, c.Orig)

		path, _, err := replacePathElement(c.Orig, buffer[:0], c.Key, c.Val, false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		rawPath, _, err := replacePathElement(origRaw, buffer[:0], c.Key, c.Val, true)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if e, a := c.ExpPath, path; bytes.Compare(e, a) != 0 {
			t.Errorf("%d, expect uri path to be %q got %q", i, e, a)
		}
		if e, a := c.ExpRawPath, rawPath; bytes.Compare(e, a) != 0 {
			t.Errorf("%d, expect uri raw path to be %q got %q", i, e, a)
		}
	}
}
