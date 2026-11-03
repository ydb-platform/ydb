#pragma once

#include "int4.h"
#include "simd_float_ops.h"
#include <cstring>

#ifdef ARCADIA_SSE
namespace NVectorDistance::NInt4::NSse::NPrivate {
    template <bool Signed>
    struct TOps : NSimd::TSseFloatOps {
        static constexpr size_t Width = 4;
        static constexpr size_t Stride = 2;

        static __m128 Load(const ui8* ptr) noexcept {
            ui16 packed;
            std::memcpy(&packed, ptr, sizeof(packed));
            const auto zero = _mm_setzero_si128();
            const auto bytes = _mm_cvtsi32_si128(packed);
            const auto mask = _mm_set1_epi8(15);
            const auto nibbles = _mm_unpacklo_epi8(_mm_and_si128(bytes, mask),
                _mm_and_si128(_mm_srli_epi16(bytes, 4), mask));
            auto values = _mm_unpacklo_epi16(_mm_unpacklo_epi8(nibbles, zero), zero);
            if constexpr (Signed) {
                const auto sign = _mm_set1_epi32(8);
                values = _mm_sub_epi32(_mm_xor_si128(values, sign), sign);
            }
            return _mm_cvtepi32_ps(values);
        }
    };
}
#endif
