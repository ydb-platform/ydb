#include "half_distance_sse.h"
#include "half_distance_simple.h"

#include <library/cpp/sse/sse.h>

#ifdef ARCADIA_SSE

namespace {
    Y_FORCE_INLINE float Hsum128(__m128 v) noexcept {
        alignas(16) float r[4];
        _mm_store_ps(r, v);
        return r[0] + r[1] + r[2] + r[3];
    }

    Y_FORCE_INLINE void Load8F16(const TFloat16* p, __m128& lo, __m128& hi) noexcept {
        lo = _mm_set_ps(static_cast<float>(p[3]), static_cast<float>(p[2]), static_cast<float>(p[1]), static_cast<float>(p[0]));
        hi = _mm_set_ps(static_cast<float>(p[7]), static_cast<float>(p[6]), static_cast<float>(p[5]), static_cast<float>(p[4]));
    }

    Y_FORCE_INLINE void Load8Bf16(const TBFloat16* p, __m128& lo, __m128& hi) noexcept {
        const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
        const __m128i z = _mm_setzero_si128();
        lo = _mm_castsi128_ps(_mm_unpacklo_epi16(z, v));
        hi = _mm_castsi128_ps(_mm_unpackhi_epi16(z, v));
    }

    template <typename T, auto Load8>
    float L1Sse(const T* lhs, const T* rhs, size_t n) noexcept {
        __m128 sum0 = _mm_setzero_ps();
        __m128 sum1 = _mm_setzero_ps();
        const __m128 absMask = _mm_castsi128_ps(_mm_set1_epi32(0x7fffffff));
        while (n >= 8) {
            __m128 a0, a1, b0, b1;
            Load8(lhs, a0, a1);
            Load8(rhs, b0, b1);
            sum0 = _mm_add_ps(sum0, _mm_and_ps(_mm_sub_ps(a0, b0), absMask));
            sum1 = _mm_add_ps(sum1, _mm_and_ps(_mm_sub_ps(a1, b1), absMask));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum128(_mm_add_ps(sum0, sum1)) + NVectorDistance::NSimple::L1(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    float L2SqrSse(const T* lhs, const T* rhs, size_t n) noexcept {
        __m128 sum0 = _mm_setzero_ps();
        __m128 sum1 = _mm_setzero_ps();
        while (n >= 8) {
            __m128 a0, a1, b0, b1;
            Load8(lhs, a0, a1);
            Load8(rhs, b0, b1);
            const __m128 d0 = _mm_sub_ps(a0, b0);
            const __m128 d1 = _mm_sub_ps(a1, b1);
            sum0 = _mm_add_ps(sum0, _mm_mul_ps(d0, d0));
            sum1 = _mm_add_ps(sum1, _mm_mul_ps(d1, d1));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum128(_mm_add_ps(sum0, sum1)) + NVectorDistance::NSimple::L2Sqr(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    float DotSse(const T* lhs, const T* rhs, size_t n) noexcept {
        __m128 sum0 = _mm_setzero_ps();
        __m128 sum1 = _mm_setzero_ps();
        while (n >= 8) {
            __m128 a0, a1, b0, b1;
            Load8(lhs, a0, a1);
            Load8(rhs, b0, b1);
            sum0 = _mm_add_ps(sum0, _mm_mul_ps(a0, b0));
            sum1 = _mm_add_ps(sum1, _mm_mul_ps(a1, b1));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        return Hsum128(_mm_add_ps(sum0, sum1)) + NVectorDistance::NSimple::Dot(lhs, rhs, n);
    }

    template <typename T, auto Load8>
    TTriWayDotProduct<float> TriWaySse(const T* lhs, const T* rhs, size_t n) noexcept {
        __m128 ll0 = _mm_setzero_ps();
        __m128 ll1 = _mm_setzero_ps();
        __m128 lr0 = _mm_setzero_ps();
        __m128 lr1 = _mm_setzero_ps();
        __m128 rr0 = _mm_setzero_ps();
        __m128 rr1 = _mm_setzero_ps();
        while (n >= 8) {
            __m128 a0, a1, b0, b1;
            Load8(lhs, a0, a1);
            Load8(rhs, b0, b1);
            ll0 = _mm_add_ps(ll0, _mm_mul_ps(a0, a0));
            ll1 = _mm_add_ps(ll1, _mm_mul_ps(a1, a1));
            lr0 = _mm_add_ps(lr0, _mm_mul_ps(a0, b0));
            lr1 = _mm_add_ps(lr1, _mm_mul_ps(a1, b1));
            rr0 = _mm_add_ps(rr0, _mm_mul_ps(b0, b0));
            rr1 = _mm_add_ps(rr1, _mm_mul_ps(b1, b1));
            lhs += 8;
            rhs += 8;
            n -= 8;
        }
        auto tail = NVectorDistance::NSimple::TriWay(lhs, rhs, n);
        tail.LL += Hsum128(_mm_add_ps(ll0, ll1));
        tail.LR += Hsum128(_mm_add_ps(lr0, lr1));
        tail.RR += Hsum128(_mm_add_ps(rr0, rr1));
        return tail;
    }
} // namespace

float L1DistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L1Sse<TFloat16, Load8F16>(lhs, rhs, length);
}
float L2SqrDistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return L2SqrSse<TFloat16, Load8F16>(lhs, rhs, length);
}
float DotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return DotSse<TFloat16, Load8F16>(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return TriWaySse<TFloat16, Load8F16>(lhs, rhs, length);
}

float L1DistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L1Sse<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
float L2SqrDistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return L2SqrSse<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
float DotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return DotSse<TBFloat16, Load8Bf16>(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return TriWaySse<TBFloat16, Load8Bf16>(lhs, rhs, length);
}

#else

float L1DistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::L1(lhs, rhs, length);
}
float L2SqrDistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::L2Sqr(lhs, rhs, length);
}
float DotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::Dot(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::TriWay(lhs, rhs, length);
}

float L1DistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::L1(lhs, rhs, length);
}
float L2SqrDistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::L2Sqr(lhs, rhs, length);
}
float DotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::Dot(lhs, rhs, length);
}
TTriWayDotProduct<float> TriWayDotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept {
    return NVectorDistance::NSimple::TriWay(lhs, rhs, length);
}

#endif
