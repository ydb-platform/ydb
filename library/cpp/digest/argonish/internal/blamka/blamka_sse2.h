#pragma once

#include <library/cpp/digest/argonish/internal/rotations/rotations_sse2.h>

namespace NArgonish {
    static inline void BlamkaG1SSE2(
        __m128i& a0, __m128i& a1, __m128i& b0, __m128i& b1,
        __m128i& c0, __m128i& c1, __m128i& d0, __m128i& d1) {
        __m128i ml = _mm_mul_epu32(a0, b0);
        ml = _mm_add_epi64(ml, ml);
        a0 = _mm_add_epi64(a0, _mm_add_epi64(b0, ml));

        ml = _mm_mul_epu32(a1, b1);
        ml = _mm_add_epi64(ml, ml);
        a1 = _mm_add_epi64(a1, _mm_add_epi64(b1, ml));

        d0 = _mm_xor_si128(d0, a0);
        d1 = _mm_xor_si128(d1, a1);

        d0 = Rotr32(d0);
        d1 = Rotr32(d1);

        ml = _mm_mul_epu32(c0, d0);
        ml = _mm_add_epi64(ml, ml);
        c0 = _mm_add_epi64(c0, _mm_add_epi64(d0, ml));

        ml = _mm_mul_epu32(c1, d1);
        ml = _mm_add_epi64(ml, ml);
        c1 = _mm_add_epi64(c1, _mm_add_epi64(ml, d1));

        b0 = _mm_xor_si128(b0, c0);
        b1 = _mm_xor_si128(b1, c1);

        b0 = Rotr24(b0);
        b1 = Rotr24(b1);
    }

    static inline void BlamkaG2SSE2(
        __m128i& a0, __m128i& a1, __m128i& b0, __m128i& b1,
        __m128i& c0, __m128i& c1, __m128i& d0, __m128i& d1) {
        __m128i ml = _mm_mul_epu32(a0, b0);
        ml = _mm_add_epi64(ml, ml);
        a0 = _mm_add_epi64(a0, _mm_add_epi64(b0, ml));

        ml = _mm_mul_epu32(a1, b1);
        ml = _mm_add_epi64(ml, ml);
        a1 = _mm_add_epi64(a1, _mm_add_epi64(b1, ml));

        d0 = _mm_xor_si128(d0, a0);
        d1 = _mm_xor_si128(d1, a1);

        d0 = Rotr16(d0);
        d1 = Rotr16(d1);

        ml = _mm_mul_epu32(c0, d0);
        ml = _mm_add_epi64(ml, ml);
        c0 = _mm_add_epi64(c0, _mm_add_epi64(d0, ml));

        ml = _mm_mul_epu32(c1, d1);
        ml = _mm_add_epi64(ml, ml);
        c1 = _mm_add_epi64(c1, _mm_add_epi64(ml, d1));

        b0 = _mm_xor_si128(b0, c0);
        b1 = _mm_xor_si128(b1, c1);

        b0 = Rotr63(b0);
        b1 = Rotr63(b1);
    }

    static inline void DiagonalizeSSE2(
        __m128i& b0, __m128i& b1, __m128i& c0, __m128i& c1, __m128i& d0, __m128i& d1) {
        __m128i tmp0 = d0;
        __m128i tmp1 = b0;
        d0 = c0;
        c0 = c1;
        c1 = d0;
        d0 = _mm_unpackhi_epi64(d1, _mm_unpacklo_epi64(tmp0, tmp0));
        d1 = _mm_unpackhi_epi64(tmp0, _mm_unpacklo_epi64(d1, d1));
        b0 = _mm_unpackhi_epi64(b0, _mm_unpacklo_epi64(b1, b1));
        b1 = _mm_unpackhi_epi64(b1, _mm_unpacklo_epi64(tmp1, tmp1));
    }

    static inline void UndiagonalizeSSE2(
        __m128i& b0, __m128i& b1, __m128i& c0, __m128i& c1, __m128i& d0, __m128i& d1) {
        __m128i tmp0 = c0;
        c0 = c1;
        c1 = tmp0;
        tmp0 = b0;
        __m128i tmp1 = d0;
        b0 = _mm_unpackhi_epi64(b1, _mm_unpacklo_epi64(b0, b0));
        b1 = _mm_unpackhi_epi64(tmp0, _mm_unpacklo_epi64(b1, b1));
        d0 = _mm_unpackhi_epi64(d0, _mm_unpacklo_epi64(d1, d1));
        d1 = _mm_unpackhi_epi64(d1, _mm_unpacklo_epi64(tmp1, tmp1));
    }
}
