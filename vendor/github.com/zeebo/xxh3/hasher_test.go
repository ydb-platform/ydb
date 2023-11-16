package xxh3

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestHasherCompat(t *testing.T) {
	buf := make([]byte, 40970)
	for i := range buf {
		buf[i] = byte(uint64(i+1) * 2654435761)
	}

	for n := range buf {
		check := func() {
			h := New()
			h.Write(buf[:n/2])
			h.Reset()
			h.Write(buf[:n])
			if exp, got := Hash(buf[:n]), h.Sum64(); exp != got {
				t.Fatalf("% -4d: %016x != %016x", n, exp, got)
			}
			if exp, got := Hash128(buf[:n]), h.Sum128(); exp != got {
				t.Fatalf("% -4d: %016x != %016x", n, exp, got)
			}
		}

		withAVX512(check)
		withAVX2(check)
		withSSE2(check)
		withGeneric(check)
	}
}

func TestHasherZeroValue(t *testing.T) {
	buf := make([]byte, 40970)
	for i := range buf {
		buf[i] = byte(uint64(i+1) * 2654435761)
	}

	for n := range buf {
		check := func() {
			var h Hasher
			h.Write(buf[:n])
			if exp, got := Hash(buf[:n]), h.Sum64(); exp != got {
				t.Fatalf("% -4d: %016x != %016x", n, exp, got)
			}
			if exp, got := Hash128(buf[:n]), h.Sum128(); exp != got {
				t.Fatalf("% -4d: %016x != %016x", n, exp, got)
			}
		}

		withAVX512(check)
		withAVX2(check)
		withSSE2(check)
		withGeneric(check)
	}
}

func TestHasherCompatSeed(t *testing.T) {
	buf := make([]byte, 40970)
	for i := range buf {
		buf[i] = byte(uint64(i+1) * 2654435761)
	}
	rng := rand.New(rand.NewSource(42))

	for n := range buf {
		seed := rng.Uint64()

		check := func() {
			h := NewSeed(seed)

			h.Write(buf[:n/2])
			h.Reset()
			h.Write(buf[:n])

			if exp, got := HashSeed(buf[:n], seed), h.Sum64(); exp != got {
				t.Fatalf("Sum64: % -4d: %016x != %016x, seed:%x", n, exp, got, seed)
				return
			}
			if exp, got := Hash128Seed(buf[:n], seed), h.Sum128(); exp != got {
				t.Errorf("Sum128: % -4d: %016x != %016x", n, exp, got)
			}
		}

		withGeneric(check)
		withAVX512(check)
		withAVX2(check)
		withSSE2(check)
	}
}

func BenchmarkHasher64(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	for n := uint(4); n <= 28; n += 2 {
		size := 1 << n
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(uint64(i+int(n)+1) * 2654435761)
		}
		seed := rng.Uint64() | 1
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var bn *testing.B
			check := func() {
				bn.Run("plain", func(b *testing.B) {
					h := New()
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						h.Reset()
						h.Write(buf[:size])
						_ = h.Sum64()
					}
				})
				bn.Run("seed", func(b *testing.B) {
					h := NewSeed(seed)
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						h.Reset()
						h.Write(buf[:size])
						_ = h.Sum64()
					}
				})
			}

			b.Run("go", func(b *testing.B) {
				bn = b
				withGeneric(check)
			})
			if hasAVX512 {
				b.Run("avx512", func(b *testing.B) {
					bn = b
					withAVX512(check)
				})
			}
			if hasAVX2 {
				b.Run("avx2", func(b *testing.B) {
					bn = b
					withAVX2(check)
				})
			}
			if hasSSE2 {
				b.Run("sse2", func(b *testing.B) {
					bn = b
					withSSE2(check)
				})
			}
		})
	}
}

func BenchmarkHasher128(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	for n := uint(4); n <= 28; n += 2 {
		size := 1 << n
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(uint64(i+int(n)+1) * 2654435761)
		}
		seed := rng.Uint64() | 1
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var bn *testing.B
			check := func() {
				bn.Run("plain", func(b *testing.B) {
					h := New()
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						h.Reset()
						h.Write(buf[:size])
						_ = h.Sum128()
					}
				})
				bn.Run("seed", func(b *testing.B) {
					h := NewSeed(seed)
					b.ReportAllocs()
					b.ResetTimer()
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						h.Reset()
						h.Write(buf[:size])
						_ = h.Sum128()
					}
				})
			}

			b.Run("go", func(b *testing.B) {
				bn = b
				withGeneric(check)
			})
			if hasAVX512 {
				b.Run("avx512", func(b *testing.B) {
					bn = b
					withAVX512(check)
				})
			}
			if hasAVX2 {
				b.Run("avx2", func(b *testing.B) {
					bn = b
					withAVX2(check)
				})
			}
			if hasSSE2 {
				b.Run("sse2", func(b *testing.B) {
					bn = b
					withSSE2(check)
				})
			}
		})
	}
}
