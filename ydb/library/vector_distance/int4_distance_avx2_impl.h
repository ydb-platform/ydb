#pragma once

#include "int4.h"
#include "simd_float_ops.h"
#include <cstring>

#ifdef _avx2_
namespace NVectorDistance::NInt4::NAvx2::NPrivate {
    template <bool Signed>
    struct TOps : NSimd::TAvx2FloatOps {
        static constexpr size_t Width = 8;
        static constexpr size_t Stride = 4;

        static __m256 Load(const ui8* ptr) noexcept {
            ui32 packed;
            std::memcpy(&packed, ptr, sizeof(packed));
            const auto bytes = _mm_cvtsi32_si128(packed);
            const auto mask = _mm_set1_epi8(15);
            const auto nibbles = _mm_unpacklo_epi8(_mm_and_si128(bytes, mask),
                _mm_and_si128(_mm_srli_epi16(bytes, 4), mask));
            auto values = _mm256_cvtepu8_epi32(nibbles);
            if constexpr (Signed) {
                const auto sign = _mm256_set1_epi32(8);
                values = _mm256_sub_epi32(_mm256_xor_si256(values, sign), sign);
            }
            return _mm256_cvtepi32_ps(values);
        }
    };
}
#endif
