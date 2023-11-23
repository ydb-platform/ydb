package xxh3

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkFixed128(b *testing.B) {
	r := func(i int) {
		bench := func(b *testing.B) {
			var acc Uint128
			d := string(make([]byte, i))
			b.Run("default", func(b *testing.B) {
				b.SetBytes(int64(i))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					acc = HashString128(d)
				}
				runtime.KeepAlive(acc)
			})
			b.Run("seed", func(b *testing.B) {
				b.SetBytes(int64(i))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					acc = HashString128Seed(d, 42)
				}
				runtime.KeepAlive(acc)
			})
		}

		if i > 240 {
			if i >= avx512Switch && hasAVX512 {
				withAVX512(func() { b.Run(fmt.Sprintf("%d-AVX512", i), bench) })
			}
			if hasAVX2 {
				withAVX2(func() { b.Run(fmt.Sprintf("%d-AVX2", i), bench) })
			}
			if hasSSE2 {
				withSSE2(func() { b.Run(fmt.Sprintf("%d-SSE2", i), bench) })
			}
		}

		withGeneric(func() { b.Run(fmt.Sprintf("%d", i), bench) })
	}
	r(0)
	r(1)
	r(2)
	r(3)
	r(4)
	r(8)
	r(9)
	r(16)
	r(17)
	r(32)
	r(33)
	r(64)
	r(65)
	r(96)
	r(97)
	r(128)
	r(129)
	r(240)
	r(241)
	r(512)
	r(1024)
	r(8192)
	r(100 * 1024)
	r(1000 * 1024)
	r(10000 * 1024)
	r(100000 * 1024)
}
