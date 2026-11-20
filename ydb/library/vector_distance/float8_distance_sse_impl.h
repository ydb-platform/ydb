#pragma once

#include "float8.h"

#include <library/cpp/sse/sse.h>

#include <cstring>

#ifdef ARCADIA_SSE
namespace NVectorDistance::NFloat8::NSse::NPrivate {

    struct TOps {
        static constexpr size_t Width = 4;

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

        template <typename T>
        static __m128 Load(const T* ptr) noexcept {
            // Read exactly four bytes: the serialized format byte is not padding.
            i32 packed;
            std::memcpy(&packed, ptr, sizeof(packed));
            const __m128i zero = _mm_setzero_si128();
            const __m128i bytes = _mm_cvtsi32_si128(packed);
            const __m128i bits = _mm_unpacklo_epi16(_mm_unpacklo_epi8(bytes, zero), zero);
            const __m128i sign = _mm_slli_epi32(_mm_and_si128(bits, _mm_set1_epi32(0x80)), 24);
            const __m128i magnitude = _mm_and_si128(bits, _mm_set1_epi32(0x7f));
            const __m128i exponent = _mm_srli_epi32(magnitude, T::MantissaBits);
            const __m128i mantissa = _mm_and_si128(magnitude, _mm_set1_epi32((1 << T::MantissaBits) - 1));
            __m128i value = _mm_add_epi32(_mm_slli_epi32(magnitude, 23 - T::MantissaBits),
                _mm_set1_epi32((127 - T::ExponentBias) << 23));

            const __m128 subnormal = _mm_mul_ps(_mm_cvtepi32_ps(mantissa),
                _mm_set1_ps(std::bit_cast<float>(static_cast<ui32>(128 - T::ExponentBias - T::MantissaBits) << 23)));
            const __m128i isSubnormal = _mm_cmpeq_epi32(exponent, zero);
            value = _mm_or_si128(_mm_and_si128(isSubnormal, _mm_castps_si128(subnormal)),
                _mm_andnot_si128(isSubnormal, value));

            __m128i special;
            __m128i isSpecial;
            if constexpr (T::FiniteOnly) {
                isSpecial = _mm_cmpeq_epi32(magnitude, _mm_set1_epi32(0x7f));
                special = _mm_set1_epi32(0x7fc00000);
            } else {
                isSpecial = _mm_cmpeq_epi32(exponent, _mm_set1_epi32(0x1f));
                special = _mm_or_si128(_mm_set1_epi32(0x7f800000),
                    _mm_slli_epi32(mantissa, 23 - T::MantissaBits));
            }
            value = _mm_or_si128(_mm_and_si128(isSpecial, special), _mm_andnot_si128(isSpecial, value));
            return _mm_castsi128_ps(_mm_or_si128(value, sign));
        }
    };

} // namespace NVectorDistance::NFloat8::NSse::NPrivate
#endif
