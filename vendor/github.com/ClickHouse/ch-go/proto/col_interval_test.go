package proto

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestInterval_Add(t *testing.T) {
	v := time.Date(2100, 10, 1, 23, 42, 12, 34145, time.UTC)
	for _, tc := range []struct {
		Scale  IntervalScale
		Result time.Time
	}{
		{
			Scale:  IntervalSecond,
			Result: v.Add(time.Second * 2),
		},
		{
			Scale:  IntervalMinute,
			Result: v.Add(time.Minute * 2),
		},
		{
			Scale:  IntervalHour,
			Result: v.Add(time.Hour * 2),
		},
		{
			Scale:  IntervalDay,
			Result: v.AddDate(0, 0, 2),
		},
		{
			Scale:  IntervalWeek,
			Result: v.AddDate(0, 0, 7*2),
		},
		{
			Scale:  IntervalMonth,
			Result: v.AddDate(0, 2, 0),
		},
		{
			Scale:  IntervalQuarter,
			Result: v.AddDate(0, 4*2, 0),
		},
		{
			Scale:  IntervalYear,
			Result: v.AddDate(2, 0, 0),
		},
	} {
		interval := Interval{
			Scale: tc.Scale,
			Value: 2,
		}
		result := interval.Add(v)
		require.True(t, result.Equal(tc.Result), "%s: %s != %s", interval.Scale, result, tc.Result)
	}
}

func TestInterval_String(t *testing.T) {
	for _, tc := range []struct {
		Scale IntervalScale
		One   string
		Many  string
	}{
		{
			Scale: IntervalSecond,
			One:   "1 second",
			Many:  "3 seconds",
		},
		{
			Scale: IntervalQuarter,
			One:   "1 quarter",
			Many:  "3 quarters",
		},
	} {
		require.Equal(t, tc.One, (Interval{Value: 1, Scale: tc.Scale}).String())
		require.Equal(t, tc.Many, (Interval{Value: 3, Scale: tc.Scale}).String())
		require.Equal(t, "-"+tc.Many, (Interval{Value: -3, Scale: tc.Scale}).String())
	}
}

func TestColInterval(t *testing.T) {
	t.Parallel()
	const rows = 50
	var data ColInterval
	for i := 0; i < rows; i++ {
		v := Interval{
			Value: 10,
		}
		data.Append(v)
		require.Equal(t, v, data.Row(i))
	}

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		t.Parallel()
		gold.Bytes(t, buf.Buf, "col_interval")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColInterval
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeInterval+"Second", dec.Type())
	})
	t.Run("ZeroRows", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColInterval
		require.NoError(t, dec.DecodeColumn(r, 0))
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColInterval
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		var dec ColInterval
		requireNoShortRead(t, buf.Buf, colAware(&dec, rows))
	})
	t.Run("ZeroRowsEncode", func(t *testing.T) {
		var v ColInterval
		v.EncodeColumn(nil) // should be no-op
	})
}
