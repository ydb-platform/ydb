package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColMapGolden(t *testing.T) {
	v := NewMap[string, string](
		new(ColStr), // k
		new(ColStr), // v
	)
	require.Equal(t, ColumnType("Map(String, String)"), v.Type())
	v.Append(map[string]string{
		"foo": "bar",
	})
	v.Append(map[string]string{
		"like": "100",
	})

	var buf Buffer
	v.EncodeColumn(&buf)
	gold.Bytes(t, buf.Buf, "col_map_of_str_str")
}

func TestColMap_Prepare(t *testing.T) {
	v := NewMap[string, string](
		new(ColStr).LowCardinality(),
		new(ColStr).LowCardinality(),
	)
	require.Equal(t, ColumnType("Map(LowCardinality(String), LowCardinality(String))"), v.Type())
	v.AppendKV([]KV[string, string]{
		{"foo", "bar"},
		{"baz", "hello"},
		{"bar", "bar"},
	})
	v.AppendKV([]KV[string, string]{
		{"like", "100"},
		{"dislike", "200"},
		{"result", "1000 - 7"},
	})
	require.NoError(t, v.Prepare())
	const rows = 2

	t.Run("Golden", func(t *testing.T) {
		var buf Buffer
		v.EncodeColumn(&buf)
		gold.Bytes(t, buf.Buf, "col_map_of_low_cardinality_str_str")
	})

	var buf Buffer
	v.EncodeColumn(&buf)
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := NewMap[string, string](
			new(ColStr).LowCardinality(),
			new(ColStr).LowCardinality(),
		)
		require.NoError(t, dec.DecodeColumn(r, rows))
		for i := 0; i < rows; i++ {
			require.Equal(t, v.Row(i), v.Row(i))
		}
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
}

func TestColMap(t *testing.T) {
	v := ColMap[string, string]{
		Keys: &ColStr{}, Values: &ColStr{},
	}
	require.Equal(t, ColumnType("Map(String, String)"), v.Type())
	v.Append(map[string]string{
		"foo": "bar",
		"baz": "hello",
	})
	v.Append(map[string]string{
		"like":    "100",
		"dislike": "200",
		"result":  "1000 - 7",
	})
	const rows = 2

	var buf Buffer
	v.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := &ColMap[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		require.NoError(t, dec.DecodeColumn(r, rows))
		for i := 0; i < rows; i++ {
			require.Equal(t, v.Row(i), v.Row(i))
		}
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := &ColMap[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColMap[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}
