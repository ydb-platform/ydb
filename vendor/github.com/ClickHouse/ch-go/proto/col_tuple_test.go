package proto

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColTuple_Rows(t *testing.T) {
	require.Equal(t, 0, ColTuple(nil).Rows())
}

func TestColTuple_DecodeColumn(t *testing.T) {
	const rows = 50
	var (
		dataStr ColStr
		dataInt ColInt64
	)
	for i := 0; i < rows; i++ {
		dataStr.Append(fmt.Sprintf("<%d>", i))
		dataInt = append(dataInt, int64(i))
	}

	data := ColTuple{&dataStr, &dataInt}

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_tuple_str_int64")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := ColTuple{new(ColStr), new(ColInt64)}
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Tuple(String, Int64)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := ColTuple{new(ColStr), new(ColInt64)}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := ColTuple{new(ColStr), new(ColInt64)}
		requireNoShortRead(t, buf.Buf, colAware(&dec, rows))
	})
}

func TestColTuple_DecodeColumn_Named(t *testing.T) {
	const rows = 50
	var (
		dataStr ColStr
		dataInt ColInt64
	)
	for i := 0; i < rows; i++ {
		dataStr.Append(fmt.Sprintf("<%d>", i))
		dataInt = append(dataInt, int64(i))
	}

	data := ColTuple{
		ColNamed[string]{Name: "strings", ColumnOf: &dataStr},
		ColNamed[int64]{Name: "ints", ColumnOf: &dataInt},
	}

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_tuple_named_str_int64")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := ColTuple{
			ColNamed[string]{Name: "strings", ColumnOf: new(ColStr)},
			ColNamed[int64]{Name: "ints", ColumnOf: new(ColInt64)},
		}
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Tuple(strings String, ints Int64)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := ColTuple{
			ColNamed[string]{Name: "strings", ColumnOf: new(ColStr)},
			ColNamed[int64]{Name: "ints", ColumnOf: new(ColInt64)},
		}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := ColTuple{
			ColNamed[string]{Name: "strings", ColumnOf: new(ColStr)},
			ColNamed[int64]{Name: "ints", ColumnOf: new(ColInt64)},
		}
		requireNoShortRead(t, buf.Buf, colAware(&dec, rows))
	})
}

func BenchmarkColTuple_DecodeColumn(b *testing.B) {
	const rows = 50_000

	elems := make([]ColUInt8, 3)
	for i := 0; i < rows; i++ {
		for j := range elems {
			elems[j] = append(elems[j], uint8(i))
		}
	}

	var data ColTuple
	for _, c := range elems {
		data = append(data, &c)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	var dec ColTuple
	for range elems {
		dec = append(dec, new(ColUInt8))
	}
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

func BenchmarkColTuple_EncodeColumn(b *testing.B) {
	const rows = 50_000

	elems := make([]ColUInt8, 3)
	for i := 0; i < rows; i++ {
		for j := range elems {
			elems[j] = append(elems[j], uint8(i))
		}
	}

	var data ColTuple
	for _, c := range elems {
		data = append(data, &c)
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
