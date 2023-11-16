package proto

import (
	"bytes"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColFixedStr_EncodeColumn(t *testing.T) {
	var data ColFixedStr
	input := []string{
		"foo",
		"bar",
		"ClickHouse",
		"one",
		"",
		"1",
	}
	rows := len(input)
	for _, s := range input {
		h := sha256.Sum256([]byte(s))
		data.Append(h[:])
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_fixed_str")
	})
	t.Run("Type", func(t *testing.T) {
		require.Equal(t, ColumnType("FixedString(32)"), data.Type())
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColFixedStr
		dec.SetSize(data.Size)
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		for i := 0; i < dec.Rows(); i++ {
			b := dec.Row(i)
			h := sha256.Sum256([]byte(input[i]))
			require.Equal(t, h[:], b)
		}
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := ColFixedStr{Size: 32}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("ZeroRows", func(t *testing.T) {
		var v ColFixedStr
		require.Equal(t, 0, v.Rows())
	})
	t.Run("AppendPanic", func(t *testing.T) {
		v := ColFixedStr{Size: 10}
		require.Panics(t, func() {
			v.Append(nil)
		})
		require.Panics(t, func() {
			v.Append(make([]byte, 9))
		})
		require.Panics(t, func() {
			v.Append(make([]byte, 11))
		})
		require.NotPanics(t, func() {
			v.Append(make([]byte, 10))
		})
	})
	t.Run("Auto", func(t *testing.T) {
		var v ColFixedStr
		v.Append(make([]byte, 10))
		require.Equal(t, 10, v.Size)
		t.Run("Reset", func(t *testing.T) {
			require.Equal(t, 1, v.Rows())
			v.Reset()
			require.Equal(t, 0, v.Rows())
			require.Equal(t, 10, v.Size)
		})
	})
}

func BenchmarkColFixedStr_DecodeColumn(b *testing.B) {
	const rows = 1_000
	data := ColFixedStr{Size: 32}
	for i := 0; i < rows; i++ {
		h := sha256.Sum256([]byte("ClickHouse не тормозит"))
		data.Buf = append(data.Buf, h[:]...)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	dec := ColFixedStr{Size: 32}
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

func BenchmarkColFixedStr_EncodeColumn(b *testing.B) {
	const rows = 1_000
	data := ColFixedStr{Size: 32}
	for i := 0; i < rows; i++ {
		h := sha256.Sum256([]byte("ClickHouse не тормозит"))
		data.Buf = append(data.Buf, h[:]...)
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
