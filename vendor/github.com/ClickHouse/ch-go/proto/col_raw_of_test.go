//go:build (amd64 || arm64 || riscv64) && !purego

package proto

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColRawOf(t *testing.T) {
	testColumn[[16]byte](t, "byte_arr_16", func() ColumnOf[[16]byte] {
		return &ColRawOf[[16]byte]{}
	}, [16]byte{1: 1}, [16]byte{10: 14})

	require.Equal(t, ColumnType("FixedString(32)"), (ColRawOf[[32]byte]{}).Type())
	require.Equal(t, ColumnType("FixedString(2)"), (ColRawOf[[2]byte]{}).Type())
}

func BenchmarkColRawOf_EncodeColumn(b *testing.B) {
	const rows = 1_000
	data := ColRawOf[[32]byte]{}
	for i := 0; i < rows; i++ {
		h := sha256.Sum256([]byte("ClickHouse не тормозит"))
		data.Append(h)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		data.EncodeColumn(&buf)
	}
}

func BenchmarkColRawOf_DecodeColumn(b *testing.B) {
	const rows = 1_000
	data := ColRawOf[[32]byte]{}
	for i := 0; i < rows; i++ {
		h := sha256.Sum256([]byte("ClickHouse не тормозит"))
		data.Append(h)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	dec := ColRawOf[[32]byte]{}
	if err := dec.DecodeColumn(r, rows); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		br.Reset(buf.Buf)
		r.raw.Reset(br)
		dec.Reset()

		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}
	}
}
