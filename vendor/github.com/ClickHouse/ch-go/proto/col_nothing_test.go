package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColNothing(t *testing.T) {
	t.Parallel()
	const rows = 50
	data := new(ColNothing).Nullable()
	for i := 0; i < rows; i++ {
		if i%2 == 0 {
			data.Append(NewNullable(Nothing{}))
		} else {
			data.Append(Null[Nothing]())
		}
		_ = data.Row(i)
	}

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		t.Parallel()
		gold.Bytes(t, buf.Buf, "col_nothing_nullable")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := new(ColNothing).Nullable()
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Nullable(Nothing)"), dec.Type())
	})
	t.Run("ZeroRows", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColNothing
		require.NoError(t, dec.DecodeColumn(r, 0))
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := new(ColNothing).Nullable()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColNothing).Nullable()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
	t.Run("ZeroRowsEncode", func(t *testing.T) {
		var v ColNothing
		v.EncodeColumn(nil) // should be no-op
	})
}
