package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

// testColumn tests column implementation.
func testColumn[T any](t *testing.T, name string, f func() ColumnOf[T], values ...T) {
	data := f()

	for _, v := range values {
		data.Append(v)
	}
	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "column_of_"+name)
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := f()
		require.NoError(t, dec.DecodeColumn(r, len(values)))
		require.Equal(t, data, dec)
	})
}

func TestColumnOfString(t *testing.T) {
	testColumn(t, "str", func() ColumnOf[string] { return new(ColStr) }, "foo", "bar", "baz")
}

func TestColArrFrom(t *testing.T) {
	var data ColStr
	arr := data.Array()
	arr.Append([]string{"foo", "bar"})
	t.Logf("%T %+v", arr.Data, arr.Data)

	_ = NewArray[string](new(ColStr))

	arrArr := NewArray[[]string](data.Array())
	arrArr.Append([][]string{
		{"foo", "bar"},
		{"baz"},
	})
	t.Log(arrArr.Type())
	_ = arrArr
}

func TestColArrOfStr(t *testing.T) {
	col := (&ColStr{}).Array()
	col.Append([]string{"foo", "bar", "foo", "foo", "baz"})
	col.Append([]string{"foo", "baz"})

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := (&ColStr{}).Array()

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		require.Equal(t, ColumnType("Array(String)"), dec.Type())
		require.Equal(t, []string{"foo", "bar", "foo", "foo", "baz"}, dec.Row(0))
		require.Equal(t, []string{"foo", "baz"}, dec.Row(1))
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).Array()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).Array()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestArrOfLowCordStr(t *testing.T) {
	col := NewArray[string](new(ColStr).LowCardinality())
	col.Append([]string{"foo", "bar", "foo", "foo", "baz"})
	col.Append([]string{"foo", "baz"})

	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_of_low_cord_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := NewArray[string](new(ColStr).LowCardinality())

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		require.Equal(t, ColumnType("Array(LowCardinality(String))"), dec.Type())
		require.Equal(t, []string{"foo", "bar", "foo", "foo", "baz"}, dec.Row(0))
		require.Equal(t, []string{"foo", "baz"}, dec.Row(1))
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := NewArray[string](new(ColStr).LowCardinality())
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := NewArray[string](new(ColStr).LowCardinality())
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestColArr_DecodeColumn(t *testing.T) {
	arr := new(ColInt8).Array()

	const rows = 5
	var values [][]int8
	for i := 0; i < rows; i++ {
		var v []int8
		for j := 0; j < i+2; j++ {
			v = append(v, 10+int8(j*2)+int8(3*i))
		}
		values = append(values, v)
	}
	for _, v := range values {
		arr.Append(v)
	}

	var buf Buffer
	arr.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_int8_manual")
	})
	t.Run("ColumnType", func(t *testing.T) {
		require.Equal(t, "Array(Int8)", arr.Type().String())
	})

	out := new(ColInt8).Array()
	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)
	require.NoError(t, out.DecodeColumn(r, rows))
	requireEqual[[]int8](t, arr, out)
}
