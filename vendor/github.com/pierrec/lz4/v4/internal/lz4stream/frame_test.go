package lz4stream

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4/internal/lz4block"
)

func TestFrameDescriptor(t *testing.T) {
	for _, tc := range []struct {
		flags             string
		bsum, csize, csum bool
		size              uint64
		bsize             uint32
	}{
		{"\x64\x40\xa7", false, false, true, 0, lz4block.Block64Kb},
		{"\x64\x50\x08", false, false, true, 0, lz4block.Block256Kb},
		{"\x64\x60\x85", false, false, true, 0, lz4block.Block1Mb},
		{"\x64\x70\xb9", false, false, true, 0, lz4block.Block4Mb},
	} {
		s := tc.flags
		label := fmt.Sprintf("%02x %02x %02x", s[0], s[1], s[2])
		t.Run(label, func(t *testing.T) {
			r := strings.NewReader(tc.flags)
			f := NewFrame()
			var fd FrameDescriptor
			if err := fd.initR(f, r); err != nil {
				t.Fatal(err)
			}

			if got, want := fd.Flags.BlockChecksum(), tc.bsum; got != want {
				t.Fatalf("got %v; want %v\n", got, want)
			}
			if got, want := fd.Flags.Size(), tc.csize; got != want {
				t.Fatalf("got %v; want %v\n", got, want)
			}
			if got, want := fd.Flags.ContentChecksum(), tc.csum; got != want {
				t.Fatalf("got %v; want %v\n", got, want)
			}
			if got, want := fd.ContentSize, tc.size; got != want {
				t.Fatalf("got %v; want %v\n", got, want)
			}
			if got, want := fd.Flags.BlockSizeIndex(), lz4block.Index(tc.bsize); got != want {
				t.Fatalf("got %v; want %v\n", got, want)
			}

			buf := new(bytes.Buffer)
			fd.initW()
			fd.Checksum = 0
			if err := fd.Write(f, buf); err != nil {
				t.Fatal(err)
			}
			// Skip the magic number set in the first 4 bytes.
			if got, want := buf.String()[4:], tc.flags; got != want {
				t.Fatalf("got %q; want %q\n", got, want)
			}
		})
	}
}

func TestFrameDataBlock(t *testing.T) {
	const sample = "abcd4566878dsvddddddqvq&&&&&((èdvshdvsvdsdh)"
	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}
	for _, tc := range []struct {
		data string
		size uint32
	}{
		{"", lz4block.Block64Kb},
		{sample, lz4block.Block64Kb},
		{strings.Repeat(sample, 10), lz4block.Block64Kb},
		{strings.Repeat(sample, 5000), lz4block.Block256Kb},
		{strings.Repeat(sample, 5000), lz4block.Block1Mb},
		{strings.Repeat(sample, 23000), lz4block.Block1Mb},
		{strings.Repeat(sample, 93000), lz4block.Block4Mb},
	} {
		label := fmt.Sprintf("%s (%d)", tc.data[:min(len(tc.data), 10)], len(tc.data))
		t.Run(label, func(t *testing.T) {
			data := tc.data
			size := tc.size
			zbuf := new(bytes.Buffer)
			f := NewFrame()
			f.Descriptor.Flags.BlockSizeIndexSet(lz4block.Index(size))

			block := NewFrameDataBlock(f)
			block.Compress(f, []byte(data), lz4block.Fast)
			if err := block.Write(f, zbuf); err != nil {
				t.Fatal(err)
			}

			if _, err := block.Read(f, zbuf, 0); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, size)
			buf, err := block.Uncompress(f, buf, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := len(buf), len(data); got != want {
				t.Fatalf("got %d; want %d", got, want)
			}
			if got, want := string(buf), data; got != want {
				t.Fatalf("got %q; want %q", got, want)
			}
		})
	}
}
