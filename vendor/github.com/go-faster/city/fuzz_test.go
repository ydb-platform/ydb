//go:build go1.18
// +build go1.18

package city

import (
	"bytes"
	"testing"
)

var _defaultCorpus = [][]byte{
	nil,
	{},
	{1, 2, 3},
	{1, 2, 3, 4, 5, 6},
	[]byte("hello"),
	[]byte("hello world"),
	bytes.Repeat([]byte("hello"), 100),
}

func FuzzHash128(f *testing.F) {
	for _, s := range _defaultCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		Hash128(data)
	})
}

func FuzzHash64(f *testing.F) {
	for _, s := range _defaultCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		Hash64(data)
	})
}

func FuzzHash32(f *testing.F) {
	for _, s := range _defaultCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		Hash32(data)
	})
}

func FuzzCH64(f *testing.F) {
	for _, s := range _defaultCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		CH64(data)
	})
}

func FuzzCH128(f *testing.F) {
	for _, s := range _defaultCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		CH128(data)
	})
}
