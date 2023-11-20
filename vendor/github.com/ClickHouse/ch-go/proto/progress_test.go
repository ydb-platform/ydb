package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestProgress_EncodeAware(t *testing.T) {
	b := new(Buffer)
	v := Progress{
		Rows:       100,
		Bytes:      608120,
		TotalRows:  1000,
		WroteRows:  441,
		WroteBytes: 91023,
	}
	v.EncodeAware(b, Version)
	gold.Bytes(t, b.Buf, "progress")

	t.Run("DecodeAware", func(t *testing.T) {
		var dec Progress
		requireDecode(t, b.Buf, aware(&dec))
		assert.Equal(t, v, dec)
		requireNoShortRead(t, b.Buf, aware(&dec))
	})
}
