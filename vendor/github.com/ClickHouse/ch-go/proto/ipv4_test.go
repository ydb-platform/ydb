package proto

import (
	"bytes"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestIPv4_String(t *testing.T) {
	for _, v := range []netip.Addr{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("1.1.1.1"),
	} {
		d := ToIPv4(v)
		require.Equal(t, v.String(), d.String())
	}
}

func TestColIPv4_NetAddr(t *testing.T) {
	input := []netip.Addr{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("127.0.0.2"),
		netip.MustParseAddr("127.0.0.3"),
	}
	var d ColIPv4
	for _, v := range input {
		d = append(d, ToIPv4(v))
	}
	var netBuf Buffer
	d.EncodeColumn(&netBuf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, netBuf.Buf, "col_ipv4_netaddr")
	})
	t.Run("Decode", func(t *testing.T) {
		br := bytes.NewReader(netBuf.Buf)
		r := NewReader(br)

		var dec ColIPv4
		require.NoError(t, dec.DecodeColumn(r, len(input)))
		var output []netip.Addr
		for _, v := range dec {
			output = append(output, v.ToIP())
		}
		require.Equal(t, input, output)
	})
}
