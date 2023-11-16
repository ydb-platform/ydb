package proto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestClientHello_Encode(t *testing.T) {
	var b Buffer
	v := ClientHello{
		Name:            "ch",
		Major:           1,
		Minor:           1,
		ProtocolVersion: 41000,
		Database:        "github",
		User:            "neo",
		Password:        "",
	}
	b.Encode(v)
	gold.Bytes(t, b.Buf, "client_hello")
	t.Run("Decode", func(t *testing.T) {
		var dec ClientHello
		buf := skipCode(t, b.Buf, int(ClientCodeHello))
		requireDecode(t, buf, &dec)
		require.Equal(t, v, dec)
		requireNoShortRead(t, buf, &dec)
	})
}

func BenchmarkClientHello_Encode(b *testing.B) {
	buf := new(Buffer)
	h := &ClientHello{
		Name:            "ClickHouse Go Faster Client",
		Major:           1,
		Minor:           1,
		ProtocolVersion: 411337,
		Database:        "github",
		User:            "neo",
		Password:        "go faster",
	}
	h.Encode(buf)
	b.SetBytes(int64(len(buf.Buf)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		h.Encode(buf)
	}
}
