package proto

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableColumns_EncodeAware(t *testing.T) {
	v := TableColumns{
		First:  "",
		Second: "columns format version: 1\n1 columns:\n`id` UInt8\n",
	}
	var b Buffer
	v.EncodeAware(&b, Version)
	t.Log(hex.Dump(b.Buf))
	t.Run("Golden", func(t *testing.T) {
		Gold(t, v)
	})
	t.Run("Decode", func(t *testing.T) {
		var dec TableColumns
		buf := skipCode(t, b.Buf, int(ServerCodeTableColumns))
		requireDecode(t, buf, aware(&dec))
		require.Equal(t, dec, v)
		requireNoShortRead(t, buf, aware(&dec))
	})
}
