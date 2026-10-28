#include "float8_distance_avx2.h"
#include "float8_distance_simd.h"

#if defined(_avx2_)
#include <immintrin.h>
#endif

namespace NVectorDistance::NFloat8::NAvx2 {

#if defined(_avx2_)
namespace {
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
            // AVX2 only; FP8 decoding does not require F16C.
            const __m256i bits = _mm256_cvtepu8_epi32(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(ptr)));
            const __m256i sign = _mm256_slli_epi32(_mm256_and_si256(bits, _mm256_set1_epi32(0x80)), 24);
            const __m256i magnitude = _mm256_and_si256(bits, _mm256_set1_epi32(0x7f));
            const __m256i exponent = _mm256_srli_epi32(magnitude, T::MantissaBits);
            const __m256i mantissa = _mm256_and_si256(magnitude, _mm256_set1_epi32((1 << T::MantissaBits) - 1));
            __m256i value = _mm256_add_epi32(_mm256_slli_epi32(magnitude, 23 - T::MantissaBits),
                _mm256_set1_epi32((127 - T::ExponentBias) << 23));

            const __m256 subnormal = _mm256_mul_ps(_mm256_cvtepi32_ps(mantissa),
                _mm256_set1_ps(std::bit_cast<float>(static_cast<ui32>(128 - T::ExponentBias - T::MantissaBits) << 23)));
            value = _mm256_blendv_epi8(value, _mm256_castps_si256(subnormal),
                _mm256_cmpeq_epi32(exponent, _mm256_setzero_si256()));

            if constexpr (T::FiniteOnly) {
                value = _mm256_blendv_epi8(value, _mm256_set1_epi32(0x7fc00000),
                    _mm256_cmpeq_epi32(magnitude, _mm256_set1_epi32(0x7f)));
            } else {
                const __m256i special = _mm256_or_si256(_mm256_set1_epi32(0x7f800000),
                    _mm256_slli_epi32(mantissa, 23 - T::MantissaBits));
                value = _mm256_blendv_epi8(value, special, _mm256_cmpeq_epi32(exponent, _mm256_set1_epi32(0x1f)));
            }
            return _mm256_castsi256_ps(_mm256_or_si256(value, sign));
        }
    };
} // namespace

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E4M3, TOps>(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E4M3, TOps>(lhs, rhs, length);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E4M3, TOps>(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return TriWay<TFloat8E4M3, TOps>(lhs, rhs, length);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E5M2, TOps>(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E5M2, TOps>(lhs, rhs, length);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E5M2, TOps>(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return TriWay<TFloat8E5M2, TOps>(lhs, rhs, length);
}

#else

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::L1(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::L2Sqr(lhs, rhs, length);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::Dot(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::TriWay(lhs, rhs, length);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::L1(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::L2Sqr(lhs, rhs, length);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::Dot(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::TriWay(lhs, rhs, length);
}

#endif

} // namespace NVectorDistance::NFloat8::NAvx2
