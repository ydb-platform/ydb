package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColLowCardinalityRaw_DecodeColumn(t *testing.T) {
	t.Run("Str", func(t *testing.T) {
		const rows = 25
		var data ColStr
		for _, v := range []string{
			"neo",
			"trinity",
			"morpheus",
		} {
			data.Append(v)
		}
		col := &ColLowCardinalityRaw{
			Index: &data,
			Key:   KeyUInt8,
		}
		for i := 0; i < rows; i++ {
			col.AppendKey(i % data.Rows())
		}

		var buf Buffer
		col.EncodeColumn(&buf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, buf.Buf, "col_low_cardinality_i_str_k_8")
		})
		t.Run("Ok", func(t *testing.T) {
			br := bytes.NewReader(buf.Buf)
			r := NewReader(br)
			dec := &ColLowCardinalityRaw{
				Index: &data,
			}
			require.NoError(t, dec.DecodeColumn(r, rows))
			require.Equal(t, col, dec)
			require.Equal(t, rows, dec.Rows())
			dec.Reset()
			require.Equal(t, 0, dec.Rows())
			require.Equal(t, ColumnTypeLowCardinality.Sub(ColumnTypeString), dec.Type())
		})
		t.Run("EOF", func(t *testing.T) {
			r := NewReader(bytes.NewReader(nil))
			dec := &ColLowCardinalityRaw{
				Index: &data,
			}
			require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
		})
		t.Run("NoShortRead", func(t *testing.T) {
			dec := &ColLowCardinalityRaw{
				Index: &data,
			}
			requireNoShortRead(t, buf.Buf, colAware(dec, rows))
		})
	})
	t.Run("Blank", func(t *testing.T) {
		// Blank columns (i.e. row count is zero) are not encoded.
		var data ColStr
		col := &ColLowCardinalityRaw{
			Index: &data,
			Key:   KeyUInt8,
		}
		var buf Buffer
		col.EncodeColumn(&buf)

		var dec ColLowCardinalityRaw
		require.NoError(t, dec.DecodeColumn(buf.Reader(), col.Rows()))
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(2)
		var dec ColLowCardinalityRaw
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidMeta", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(0)
		var dec ColLowCardinalityRaw
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidKeyType", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt64+1))
		var dec ColLowCardinalityRaw
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
}
