package proto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func Test_putUInt256(t *testing.T) {
	v := UInt256FromInt(100)
	buf := make([]byte, 32)
	binPutUInt256(buf, v)
	gold.Bytes(t, buf, "uint256_100")
	dec := binUInt256(buf)
	require.Equal(t, v, dec)
}

func Benchmark_PutUInt256(b *testing.B) {
	buf := make([]byte, 256/8)
	var v UInt256
	b.ReportAllocs()
	b.SetBytes(int64(len(buf)))

	for i := 0; i < b.N; i++ {
		binPutUInt256(buf, v)
	}
}

func Benchmark_UInt256(b *testing.B) {
	buf := make([]byte, 256/8)
	binPutUInt256(buf, UInt256{})
	b.ReportAllocs()
	b.SetBytes(int64(len(buf)))

	var v UInt256
	for i := 0; i < b.N; i++ {
		v = binUInt256(buf)
	}
	_ = v
}
