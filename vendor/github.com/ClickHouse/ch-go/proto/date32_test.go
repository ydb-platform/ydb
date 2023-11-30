package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDate32_Time(t *testing.T) {
	t.Parallel()
	t.Run("Single", func(t *testing.T) {
		v := time.Date(2011, 10, 10, 14, 59, 31, 401235, time.UTC)
		d := ToDate32(v)
		assert.Equal(t, Date32(15257), d) // SELECT toInt64(Date32('2011-10-10'))
		assert.Equal(t, NewDate32(2011, 10, 10), d)
		assert.Equal(t, d.String(), "2011-10-10")
		assert.Equal(t, d, ToDate32(d.Time()))
	})
	t.Run("Range", func(t *testing.T) {
		t.Parallel()
		var (
			start = time.Date(1925, 1, 1, 0, 0, 0, 0, time.UTC)
			end   = time.Date(2283, 11, 11, 0, 0, 0, 0, time.UTC)
		)
		for v := start; v.Before(end); v = v.AddDate(0, 0, 1) {
			date := ToDate32(v)
			newTime := date.Time()
			require.True(t, newTime.Equal(v))
			newDate32 := NewDate32(newTime.Year(), newTime.Month(), newTime.Day())
			require.Equal(t, date, newDate32)
			require.Equal(t, v.Format("2006-01-02"), date.String())
		}
	})
}

func TestToDate32(t *testing.T) {
	for _, tc := range []struct {
		Value string
	}{
		{Value: "2006-01-02T06:04:03+07:00"},
		{Value: "2008-01-02T06:44:15+03:00"},
		{Value: "2009-01-01T06:03:31+12:00"},
		{Value: "2006-12-31T22:04:41-06:30"},
	} {
		t.Run(tc.Value, func(t *testing.T) {
			v, err := time.Parse(time.RFC3339, tc.Value)
			require.NoError(t, err)
			d := ToDate32(v)
			expected := NewDate32(v.Year(), v.Month(), v.Day())
			assert.Equal(t, v.Format(DateLayout), d.String())
			assert.Equal(t, expected.String(), d.String())
			assert.Equal(t, expected, d)
		})
	}
}

func BenchmarkDate32_Time(b *testing.B) {
	b.ReportAllocs()

	v := Date32(100)
	var t time.Time
	for i := 0; i < b.N; i++ {
		t = v.Time()
	}
	_ = t.IsZero()
}
