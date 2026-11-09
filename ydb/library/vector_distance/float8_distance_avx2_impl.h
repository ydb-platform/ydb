#pragma once

#include "float8.h"

#if defined(_avx2_)
#include <immintrin.h>
#endif

#ifdef _avx2_
namespace NVectorDistance::NFloat8::NAvx2::NPrivate {

    struct TOps {
        static constexpr size_t Width = 8;

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

        template <typename T>
        static __m256 Load(const T* ptr) noexcept {
            const __m128i bytes = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(ptr));
            const __m128i highBytes = _mm_unpacklo_epi8(_mm_setzero_si128(), bytes);
            const __m128i magnitude = _mm_and_si128(highBytes, _mm_set1_epi16(0x7f00));
            if constexpr (T::FiniteOnly) {
                // E4M3FN is binary16 scaled by 2^8, except its two NaN codes.
                const __m128i sign = _mm_and_si128(highBytes, _mm_set1_epi16(static_cast<i16>(0x8000)));
                const __m128i isNaN = _mm_cmpeq_epi16(magnitude, _mm_set1_epi16(0x7f00));
                const __m128i halfMagnitude = _mm_xor_si128(_mm_srli_epi16(magnitude, 1),
                    _mm_and_si128(isNaN, _mm_set1_epi16(0x4180)));
                return _mm256_mul_ps(_mm256_cvtph_ps(_mm_or_si128(sign, halfMagnitude)), _mm256_set1_ps(256.0f));
            } else {
                // E5M2 is the high byte of binary16. F16C quiets signaling NaNs;
                // restore their payload bits to match scalar FP8 conversion.
                const __m128i isSignalingNaN = _mm_cmpeq_epi16(magnitude, _mm_set1_epi16(0x7d00));
                const __m128i half = _mm_or_si128(highBytes, _mm_and_si128(isSignalingNaN, _mm_set1_epi16(0x0200)));
                const __m256 value = _mm256_cvtph_ps(half);
                const __m256i quietBit = _mm256_and_si256(_mm256_cvtepi16_epi32(isSignalingNaN), _mm256_set1_epi32(0x00400000));
                return _mm256_castsi256_ps(_mm256_andnot_si256(quietBit, _mm256_castps_si256(value)));
            }
        }
    };

} // namespace NVectorDistance::NFloat8::NAvx2::NPrivate
#endif
