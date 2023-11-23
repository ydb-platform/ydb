package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateTime_Time(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		v := time.Unix(1546290000, 0).UTC()
		d := ToDateTime(v)
		assert.Equal(t, int32(1546290000), int32(d))
	})
	t.Run("Zero", func(t *testing.T) {
		v := time.Time{}
		d := ToDateTime(v)
		assert.Equal(t, int32(0), int32(d))
	})
}
