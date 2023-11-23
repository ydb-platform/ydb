package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateTime64_Time(t *testing.T) {
	for _, p := range []Precision{
		PrecisionSecond,
		1,
		PrecisionMilli,
		PrecisionMicro,
		PrecisionNano,
		8,
	} {
		t.Run(p.Duration().String(), func(t *testing.T) {
			for _, v := range []time.Time{
				{}, // zero time
				time.Unix(1546290000, 0).UTC(),
			} {
				d := ToDateTime64(v, p)
				vt := d.Time(p)
				assert.Equal(t, ToDateTime64(v, p), d)
				assert.Equal(t, v.Unix(), vt.Unix())
				assert.True(t, p.Valid())
			}
		})
	}
	t.Run("Duration", func(t *testing.T) {
		assert.Equal(t, time.Second, PrecisionSecond.Duration(), "sec")
		assert.Equal(t, time.Nanosecond, PrecisionNano.Duration(), "ns")
	})
}
