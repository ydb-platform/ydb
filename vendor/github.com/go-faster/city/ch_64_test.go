package city

import (
	"testing"
)

func BenchmarkClickHouse64(b *testing.B) {
	setup()
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		CH64(data[:1024])
	}
}
