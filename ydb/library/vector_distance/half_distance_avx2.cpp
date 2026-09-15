#include "half_distance_avx2.h"
#include "half_distance_sse.h"

#if defined(_avx2_)

#include <immintrin.h>
#include <util/system/compiler.h>

namespace {
    Y_FORCE_INLINE float Hsum256(__m256 v) noexcept {
        const __m128 s = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
        alignas(16) float r[4];
        _mm_store_ps(r, s);
        return r[0] + r[1] + r[2] + r[3];
    }

    Y_FORCE_INLINE __m256 Load8F16(const TFloat16* p) noexcept {
        return _mm256_cvtph_ps(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p)));
    }

    Y_FORCE_INLINE __m256 Load8Bf16(const TBFloat16* p) noexcept {
        const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
        return _mm256_castsi256_ps(_mm256_slli_epi32(_mm256_cvtepu16_epi32(v), 16));
    }

    template <typename T, auto Load8>
    float L1Avx2(const T* lhs, const T* rhs, size_t n) noexcept {
        __m256 sum0 = _mm256_setzero_ps();
        __m256 sum1 = _mm256_setzero_ps();
        const __m256 absMask = _mm256_castsi256_ps(_mm256_set1_epi32(0x7fffffff));
        while (n >= 16) {
            const __m256 d0 = _mm256_sub_ps(Load8(lhs), Load8(rhs));
            const __m256 d1 = _mm256_sub_ps(Load8(lhs + 8), Load8(rhs + 8));
            sum0 = _mm256_add_ps(sum0, _mm256_and_ps(d0, absMask));
            sum1 = _mm256_add_ps(sum1, _mm256_and_ps(d1, absMask));
            lhs += 16;
            rhs += 16;
            n -= 16;
        }
        if (n >= 8) {
            const __m256 d0 = _mm256_sub_ps(Load8(lhs), Load8(rhs));
            sum0 = _mm256_add_ps(sum0, _mm256_and_ps(d0, absMask));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum256(_mm256_add_ps(sum0, sum1)) + L1DistanceSse(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    float L2SqrAvx2(const T* lhs, const T* rhs, size_t n) noexcept {
        __m256 sum0 = _mm256_setzero_ps();
        __m256 sum1 = _mm256_setzero_ps();
        while (n >= 16) {
            const __m256 d0 = _mm256_sub_ps(Load8(lhs), Load8(rhs));
            const __m256 d1 = _mm256_sub_ps(Load8(lhs + 8), Load8(rhs + 8));
            sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(d0, d0));
            sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(d1, d1));
            lhs += 16;
            rhs += 16;
            n -= 16;
        }
        if (n >= 8) {
            const __m256 d0 = _mm256_sub_ps(Load8(lhs), Load8(rhs));
            sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(d0, d0));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum256(_mm256_add_ps(sum0, sum1)) + L2SqrDistanceSse(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    float DotAvx2(const T* lhs, const T* rhs, size_t n) noexcept {
        __m256 sum0 = _mm256_setzero_ps();
        __m256 sum1 = _mm256_setzero_ps();
        while (n >= 16) {
            sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(Load8(lhs), Load8(rhs)));
            sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(Load8(lhs + 8), Load8(rhs + 8)));
            lhs += 16;
            rhs += 16;
            n -= 16;
        }
        if (n >= 8) {
            sum0 = _mm256_add_ps(sum0, _mm256_mul_ps(Load8(lhs), Load8(rhs)));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum256(_mm256_add_ps(sum0, sum1)) + DotProductSse(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    TTriWayDotProduct<float> TriWayAvx2(const T* lhs, const T* rhs, size_t n) noexcept {
        __m256 ll0 = _mm256_setzero_ps();
        __m256 ll1 = _mm256_setzero_ps();
        __m256 lr0 = _mm256_setzero_ps();
        __m256 lr1 = _mm256_setzero_ps();
        __m256 rr0 = _mm256_setzero_ps();
        __m256 rr1 = _mm256_setzero_ps();
        while (n >= 16) {
            const __m256 a0 = Load8(lhs);
            const __m256 b0 = Load8(rhs);
            const __m256 a1 = Load8(lhs + 8);
            const __m256 b1 = Load8(rhs + 8);
            ll0 = _mm256_add_ps(ll0, _mm256_mul_ps(a0, a0));
            ll1 = _mm256_add_ps(ll1, _mm256_mul_ps(a1, a1));
            lr0 = _mm256_add_ps(lr0, _mm256_mul_ps(a0, b0));
            lr1 = _mm256_add_ps(lr1, _mm256_mul_ps(a1, b1));
            rr0 = _mm256_add_ps(rr0, _mm256_mul_ps(b0, b0));
            rr1 = _mm256_add_ps(rr1, _mm256_mul_ps(b1, b1));
            lhs += 16;
            rhs += 16;
            n -= 16;
        }
        if (n >= 8) {
            const __m256 a0 = Load8(lhs);
            const __m256 b0 = Load8(rhs);
            ll0 = _mm256_add_ps(ll0, _mm256_mul_ps(a0, a0));
            lr0 = _mm256_add_ps(lr0, _mm256_mul_ps(a0, b0));
            rr0 = _mm256_add_ps(rr0, _mm256_mul_ps(b0, b0));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        auto tail = TriWayDotProductSse(lhs, rhs, n);
        tail.LL += Hsum256(_mm256_add_ps(ll0, ll1));
        tail.LR += Hsum256(_mm256_add_ps(lr0, lr1));
        tail.RR += Hsum256(_mm256_add_ps(rr0, rr1));
        return tail;
    }
} // namespace

float L1DistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L1Avx2<TFloat16, Load8F16>(lhs, rhs, length);
}
float L2SqrDistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L2SqrAvx2<TFloat16, Load8F16>(lhs, rhs, length);
}
float DotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return DotAvx2<TFloat16, Load8F16>(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return TriWayAvx2<TFloat16, Load8F16>(lhs, rhs, length);
}

float L1DistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L1Avx2<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
float L2SqrDistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L2SqrAvx2<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
float DotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return DotAvx2<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return TriWayAvx2<TBFloat16, Load8Bf16>(lhs, rhs, length);
}

#else

float L1DistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L1DistanceSse(lhs, rhs, length);
}
float L2SqrDistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L2SqrDistanceSse(lhs, rhs, length);
}
float DotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return DotProductSse(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return TriWayDotProductSse(lhs, rhs, length);
}

float L1DistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L1DistanceSse(lhs, rhs, length);
}
float L2SqrDistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L2SqrDistanceSse(lhs, rhs, length);
}
float DotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return DotProductSse(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return TriWayDotProductSse(lhs, rhs, length);
}

#endif
