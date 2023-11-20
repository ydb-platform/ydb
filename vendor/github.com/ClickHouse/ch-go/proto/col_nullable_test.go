package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColNullable_EncodeColumn(t *testing.T) {
	const rows = 10
	data := new(ColStr).Nullable()
	values := []Nullable[string]{
		{Value: "value1", Set: true},
		{Value: "value2", Set: true},
		{},
		{Value: "value3", Set: true},
		{},
		{Value: "", Set: true},
		{Value: "", Set: true},
		{Value: "value4", Set: true},
		{},
		{Value: "value54", Set: true},
	}
	for _, v := range values {
		data.Append(v)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_nullable_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := new(ColStr).Nullable()
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		requireEqual[Nullable[string]](t, data, dec)
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Nullable(String)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := new(ColStr).Nullable()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColStr).Nullable()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}

func TestColNullable(t *testing.T) {
	col := NewColNullable[string](new(ColStr))
	v := []Nullable[string]{
		NewNullable("foo"),
		Null[string](),
		NewNullable("bar"),
		NewNullable("baz"),
	}
	col.AppendArr(v)

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_nullable_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := &ColNullable[string]{Values: new(ColStr)}

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		for i, s := range v {
			assert.Equal(t, s, col.Row(i))
		}
		assert.Equal(t, ColumnType("Nullable(String)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := &ColNullable[string]{Values: new(ColStr)}
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColNullable[string]{Values: new(ColStr)}
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}
