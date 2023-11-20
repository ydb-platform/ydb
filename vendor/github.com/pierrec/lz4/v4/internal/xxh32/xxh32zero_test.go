package xxh32_test

import (
	"encoding/binary"
	"hash/crc32"
	"hash/fnv"
	"testing"

	"github.com/pierrec/lz4/v4/internal/xxh32"
)

type test struct {
	sum  uint32
	data string
}

var testdata = []test{
	{0x02cc5d05, ""},
	{0x550d7456, "a"},
	{0x4999fc53, "ab"},
	{0x32d153ff, "abc"},
	{0xa3643705, "abcd"},
	{0x9738f19b, "abcde"},
	{0x8b7cd587, "abcdef"},
	{0x9dd093b3, "abcdefg"},
	{0x0bb3c6bb, "abcdefgh"},
	{0xd03c13fd, "abcdefghi"},
	{0x8b988cfe, "abcdefghij"},
	{0x9d2d8b62, "abcdefghijklmnop"},
	{0x42ae804d, "abcdefghijklmnopqrstuvwxyz0123456789"},
	{0x62b4ed00, "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."},
}

func TestZeroBlockSize(t *testing.T) {
	var xxh xxh32.XXHZero
	if s := xxh.BlockSize(); s <= 0 {
		t.Errorf("invalid BlockSizeIndex: %d", s)
	}
}

func TestZeroSize(t *testing.T) {
	var xxh xxh32.XXHZero
	if s := xxh.Size(); s != 4 {
		t.Errorf("invalid Size: got %d expected 4", s)
	}
}

func TestZeroData(t *testing.T) {
	for _, td := range testdata {
		var xxh xxh32.XXHZero
		data := []byte(td.data)
		_, _ = xxh.Write(data)

		if got, want := xxh.Sum32(), td.sum; got != want {
			t.Fatalf("got %x; want %x", got, want)
		}
		if got, want := xxh32.ChecksumZero(data), td.sum; got != want {
			t.Errorf("got %x; want %x", got, want)
		}
	}
}

func TestZeroSplitData(t *testing.T) {
	for _, td := range testdata {
		var xxh xxh32.XXHZero
		data := []byte(td.data)
		l := len(data) / 2
		_, _ = xxh.Write(data[0:l])
		_, _ = xxh.Write(data[l:])

		if got, want := xxh.Sum32(), td.sum; got != want {
			t.Fatalf("got %x; want %x", got, want)
		}
	}
}

func TestZeroSum(t *testing.T) {
	for _, td := range testdata {
		var xxh xxh32.XXHZero
		data := []byte(td.data)
		_, _ = xxh.Write(data)
		b := xxh.Sum(data)
		h := binary.LittleEndian.Uint32(b[len(data):])
		if got, want := h, td.sum; got != want {
			t.Fatalf("got %x; want %x", got, want)
		}
	}
}

func TestZeroChecksum(t *testing.T) {
	for _, td := range testdata {
		data := []byte(td.data)
		h := xxh32.ChecksumZero(data)
		if got, want := h, td.sum; got != want {
			t.Errorf("got %x; want %x", got, want)
		}
	}
}

func TestZeroReset(t *testing.T) {
	var xxh xxh32.XXHZero
	for _, td := range testdata {
		_, _ = xxh.Write([]byte(td.data))
		h := xxh.Sum32()
		if got, want := h, td.sum; got != want {
			t.Errorf("got %x; want %x", got, want)
		}
		xxh.Reset()
	}
}

func TestNil(t *testing.T) {
	want := xxh32.ChecksumZero([]byte(""))

	var xxh xxh32.XXHZero
	xxh.Write(nil)
	got := xxh.Sum32()
	if got != want {
		t.Errorf("got %x; want %x", got, want)
	}

	got = xxh32.ChecksumZero(nil)
	if got != want {
		t.Errorf("got %x; want %x", got, want)
	}
}

func TestUnaligned(t *testing.T) {
	zeros := make([]byte, 100)
	ha := xxh32.ChecksumZero(zeros[:len(zeros)-1])
	hu := xxh32.ChecksumZero(zeros[1:])
	if ha != hu {
		t.Errorf("mismatch: %x != %x", ha, hu)
	}

	var xxh xxh32.XXHZero
	xxh.Write(zeros[:len(zeros)-1])
	ha = xxh.Sum32()

	xxh.Reset()
	xxh.Write(zeros[1:])
	hu = xxh32.ChecksumZero(zeros[1:])

	if ha != hu {
		t.Errorf("mismatch: %x != %x", ha, hu)
	}
}

///////////////////////////////////////////////////////////////////////////////
// Benchmarks
//
var testdata1 = []byte(testdata[len(testdata)-1].data)

func Benchmark_XXH32(b *testing.B) {
	var h xxh32.XXHZero
	for n := 0; n < b.N; n++ {
		_, _ = h.Write(testdata1)
		h.Sum32()
		h.Reset()
	}
}

func Benchmark_XXH32_Checksum(b *testing.B) {
	for n := 0; n < b.N; n++ {
		xxh32.ChecksumZero(testdata1)
	}
}

// The following two benchmark the case where 3/4 calls are not 4-byte-aligned.
func Benchmark_XXH32Unaligned(b *testing.B) {
	var h xxh32.XXHZero
	for n := 0; n < b.N; n++ {
		_, _ = h.Write(testdata1[n%4:])
		h.Sum32()
		h.Reset()
	}
}

func Benchmark_XXH32_ChecksumUnaligned(b *testing.B) {
	for n := 0; n < b.N; n++ {
		xxh32.ChecksumZero(testdata1[n%4:])
	}
}

func Benchmark_CRC32(b *testing.B) {
	t := crc32.MakeTable(0)
	for i := 0; i < b.N; i++ {
		crc32.Checksum(testdata1, t)
	}
}

func Benchmark_Fnv32(b *testing.B) {
	h := fnv.New32()
	for i := 0; i < b.N; i++ {
		_, _ = h.Write(testdata1)
		h.Sum32()
		h.Reset()
	}
}
