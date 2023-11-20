package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColRaw(t *testing.T) {
	v := ColRaw{
		T:     ColumnType("Foo(5)"),
		Size:  5,
		Data:  []byte{1, 2, 3, 4, 5},
		Count: 1,
	}

	r := require.New(t)
	r.Equal(v.T, v.Type())
	r.Equal(1, v.Rows())

	b := new(Buffer)
	v.EncodeColumn(b)

	dec := ColRaw{T: v.T, Size: v.Size}
	r.NoError(dec.DecodeColumn(b.Reader(), 1))
	r.Equal(v, dec)

	dec.Reset()
	r.Equal(0, dec.Rows())
	r.Len(dec.Data, 0)
}

func BenchmarkColRaw_EncodeColumn(b *testing.B) {
	buf := new(Buffer)
	v := ColRaw{
		Data: make([]byte, 1024),
	}

	b.ReportAllocs()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		buf.Reset()
		v.EncodeColumn(buf)
	}
}

func BenchmarkColRaw_DecodeColumn(b *testing.B) {
	const (
		rows = 1_000
		size = 64
		data = size * rows
	)

	raw := make([]byte, data)
	br := bytes.NewReader(raw)
	r := NewReader(br)

	b.ReportAllocs()
	b.SetBytes(data)

	dec := ColRaw{
		T:    ColumnTypeUInt64,
		Size: size,
	}
	for i := 0; i < b.N; i++ {
		br.Reset(raw)
		r.raw.Reset(br)
		dec.Reset()

		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}
	}
}
