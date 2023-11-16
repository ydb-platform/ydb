package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfile_EncodeAware(t *testing.T) {
	p := Profile{
		Rows:                      1234,
		Blocks:                    235123,
		Bytes:                     424,
		AppliedLimit:              true,
		RowsBeforeLimit:           2341,
		CalculatedRowsBeforeLimit: false,
	}
	var b Buffer
	p.EncodeAware(&b, Version)
	Gold(t, &p)

	t.Run("Decode", func(t *testing.T) {
		buf := skipCode(t, b.Buf, int(ServerCodeProfile))
		var dec Profile
		requireDecode(t, buf, aware(&dec))
		require.Equal(t, p, dec)
		requireNoShortRead(t, buf, aware(&dec))
	})
}
