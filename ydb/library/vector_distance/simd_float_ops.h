#pragma once

#include <library/cpp/sse/sse.h>
#if defined(_avx2_)
#include <immintrin.h>
#endif

namespace NVectorDistance::NSimd {

#ifdef ARCADIA_SSE
    struct TSseFloatOps {
        static __m128 Zero() noexcept { return _mm_setzero_ps(); }
        static __m128 Add(__m128 a, __m128 b) noexcept { return _mm_add_ps(a, b); }
        static __m128 Sub(__m128 a, __m128 b) noexcept { return _mm_sub_ps(a, b); }
        static __m128 Mul(__m128 a, __m128 b) noexcept { return _mm_mul_ps(a, b); }
        static __m128 Abs(__m128 a) noexcept { return _mm_and_ps(a, _mm_castsi128_ps(_mm_set1_epi32(0x7fffffff))); }

        static float Sum(__m128 a) noexcept {
            alignas(16) float values[4];
            _mm_store_ps(values, a);
            return values[0] + values[1] + values[2] + values[3];
        }

    };
#endif

#ifdef _avx2_
    struct TAvx2FloatOps {
        static __m256 Zero() noexcept { return _mm256_setzero_ps(); }
        static __m256 Add(__m256 a, __m256 b) noexcept { return _mm256_add_ps(a, b); }
        static __m256 Sub(__m256 a, __m256 b) noexcept { return _mm256_sub_ps(a, b); }
        static __m256 Mul(__m256 a, __m256 b) noexcept { return _mm256_mul_ps(a, b); }
        static __m256 Abs(__m256 a) noexcept { return _mm256_and_ps(a, _mm256_castsi256_ps(_mm256_set1_epi32(0x7fffffff))); }

        static float Sum(__m256 a) noexcept {
            const __m128 sum = _mm_add_ps(_mm256_castps256_ps128(a), _mm256_extractf128_ps(a, 1));
            alignas(16) float values[4];
            _mm_store_ps(values, sum);
            return values[0] + values[1] + values[2] + values[3];
        }

    };
#endif

} // namespace NVectorDistance::NSimd
