package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestLowCardinalityOf(t *testing.T) {
	v := NewLowCardinality[string](new(ColStr))

	require.NoError(t, v.Prepare())
	require.Equal(t, ColumnType("LowCardinality(String)"), v.Type())
}

func TestLowCardinalityOfStr(t *testing.T) {
	col := (&ColStr{}).LowCardinality()
	v := []string{"foo", "bar", "foo", "foo", "baz"}
	col.AppendArr(v)

	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_low_cardinality_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := (&ColStr{}).LowCardinality()

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		for i, s := range v {
			assert.Equal(t, s, col.Row(i))
		}
		assert.Equal(t, ColumnType("LowCardinality(String)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).LowCardinality()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).LowCardinality()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestArrLowCardinalityStr(t *testing.T) {
	// Array(LowCardinality(String))
	data := [][]string{
		{"foo", "bar", "baz"},
		{"foo"},
		{"bar", "bar"},
		{"foo", "foo"},
		{"bar", "bar", "bar", "bar"},
	}
	col := new(ColStr).LowCardinality().Array()
	rows := len(data)
	for _, v := range data {
		col.Append(v)
	}
	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_low_cardinality_u8_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := new(ColStr).LowCardinality().Array()
		require.NoError(t, dec.DecodeColumn(r, rows))
		requireEqual[[]string](t, col, dec)
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Array(LowCardinality(String))"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := new(ColStr).LowCardinality().Array()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColStr).LowCardinality().Array()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}

func TestColLowCardinality_DecodeColumn(t *testing.T) {
	t.Run("Str", func(t *testing.T) {
		const rows = 25
		values := []string{
			"neo",
			"trinity",
			"morpheus",
		}
		col := new(ColStr).LowCardinality()
		for i := 0; i < rows; i++ {
			col.Append(values[i%len(values)])
		}
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, buf.Buf, "col_low_cardinality_i_str_k_8")
		})
		t.Run("Ok", func(t *testing.T) {
			br := bytes.NewReader(buf.Buf)
			r := NewReader(br)
			dec := new(ColStr).LowCardinality()
			require.NoError(t, dec.DecodeColumn(r, rows))
			requireEqual[string](t, col, dec)
			dec.Reset()
			require.Equal(t, 0, dec.Rows())
			require.Equal(t, ColumnTypeLowCardinality.Sub(ColumnTypeString), dec.Type())
		})
		t.Run("EOF", func(t *testing.T) {
			r := NewReader(bytes.NewReader(nil))
			dec := new(ColStr).LowCardinality()
			require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
		})
		t.Run("NoShortRead", func(t *testing.T) {
			dec := new(ColStr).LowCardinality()
			requireNoShortRead(t, buf.Buf, colAware(dec, rows))
		})
	})
	t.Run("Blank", func(t *testing.T) {
		// Blank columns (i.e. row count is zero) are not encoded.
		col := new(ColStr).LowCardinality()
		var buf Buffer
		require.NoError(t, col.Prepare())
		col.EncodeColumn(&buf)

		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), col.Rows()))
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(2)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidMeta", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(0)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidKeyType", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt64+1))
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
}
