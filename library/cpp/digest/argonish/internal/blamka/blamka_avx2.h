#pragma once

#include <immintrin.h>
#include <library/cpp/digest/argonish/internal/rotations/rotations_avx2.h>

namespace NArgonish {
    static inline void BlamkaG1AVX2(
        __m256i& a0, __m256i& a1, __m256i& b0, __m256i& b1,
        __m256i& c0, __m256i& c1, __m256i& d0, __m256i& d1) {
        __m256i ml = _mm256_mul_epu32(a0, b0);
        ml = _mm256_add_epi64(ml, ml);
        a0 = _mm256_add_epi64(a0, _mm256_add_epi64(b0, ml));
        d0 = _mm256_xor_si256(d0, a0);
        d0 = Rotr32(d0);

        ml = _mm256_mul_epu32(c0, d0);
        ml = _mm256_add_epi64(ml, ml);
        c0 = _mm256_add_epi64(c0, _mm256_add_epi64(d0, ml));

        b0 = _mm256_xor_si256(b0, c0);
        b0 = Rotr24(b0);

        ml = _mm256_mul_epu32(a1, b1);
        ml = _mm256_add_epi64(ml, ml);
        a1 = _mm256_add_epi64(a1, _mm256_add_epi64(b1, ml));
        d1 = _mm256_xor_si256(d1, a1);
        d1 = Rotr32(d1);

        ml = _mm256_mul_epu32(c1, d1);
        ml = _mm256_add_epi64(ml, ml);
        c1 = _mm256_add_epi64(c1, _mm256_add_epi64(d1, ml));

        b1 = _mm256_xor_si256(b1, c1);
        b1 = Rotr24(b1);
    }

    static inline void BlamkaG2AVX2(
        __m256i& a0, __m256i& a1, __m256i& b0, __m256i& b1,
        __m256i& c0, __m256i& c1, __m256i& d0, __m256i& d1) {
        __m256i ml = _mm256_mul_epu32(a0, b0);
        ml = _mm256_add_epi64(ml, ml);
        a0 = _mm256_add_epi64(a0, _mm256_add_epi64(b0, ml));
        d0 = _mm256_xor_si256(d0, a0);
        d0 = Rotr16(d0);

        ml = _mm256_mul_epu32(c0, d0);
        ml = _mm256_add_epi64(ml, ml);
        c0 = _mm256_add_epi64(c0, _mm256_add_epi64(d0, ml));
        b0 = _mm256_xor_si256(b0, c0);
        b0 = Rotr63(b0);

        ml = _mm256_mul_epu32(a1, b1);
        ml = _mm256_add_epi64(ml, ml);
        a1 = _mm256_add_epi64(a1, _mm256_add_epi64(b1, ml));
        d1 = _mm256_xor_si256(d1, a1);
        d1 = Rotr16(d1);

        ml = _mm256_mul_epu32(c1, d1);
        ml = _mm256_add_epi64(ml, ml);
        c1 = _mm256_add_epi64(c1, _mm256_add_epi64(d1, ml));
        b1 = _mm256_xor_si256(b1, c1);
        b1 = Rotr63(b1);
    }

    /* a = ( v0,  v1,  v2,  v3) */
    /* b = ( v4,  v5,  v6,  v7) */
    /* c = ( v8,  v9, v10, v11) */
    /* d = (v12, v13, v14, v15) */
    static inline void DiagonalizeAVX21(
        __m256i& b0, __m256i& c0, __m256i& d0, __m256i& b1, __m256i& c1, __m256i& d1) {
        /* (v4, v5, v6, v7) -> (v5, v6, v7, v4) */
        b0 = _mm256_permute4x64_epi64(b0, _MM_SHUFFLE(0, 3, 2, 1));
        /* (v8, v9, v10, v11) -> (v10, v11, v8, v9) */
        c0 = _mm256_permute4x64_epi64(c0, _MM_SHUFFLE(1, 0, 3, 2));
        /* (v12, v13, v14, v15) -> (v15, v12, v13, v14) */
        d0 = _mm256_permute4x64_epi64(d0, _MM_SHUFFLE(2, 1, 0, 3));

        b1 = _mm256_permute4x64_epi64(b1, _MM_SHUFFLE(0, 3, 2, 1));
        c1 = _mm256_permute4x64_epi64(c1, _MM_SHUFFLE(1, 0, 3, 2));
        d1 = _mm256_permute4x64_epi64(d1, _MM_SHUFFLE(2, 1, 0, 3));
    }

    static inline void DiagonalizeAVX22(
        __m256i& b0, __m256i& b1, __m256i& c0, __m256i& c1, __m256i& d0, __m256i& d1) {
        /* (v4, v5, v6, v7) -> (v5, v6, v7, v4) */
        __m256i tmp1 = _mm256_blend_epi32(b0, b1, 0b11001100);        /* v4v7 */
        __m256i tmp2 = _mm256_blend_epi32(b0, b1, 0b00110011);        /* v6v5 */
        b1 = _mm256_permute4x64_epi64(tmp1, _MM_SHUFFLE(2, 3, 0, 1)); /* v7v4 */
        b0 = _mm256_permute4x64_epi64(tmp2, _MM_SHUFFLE(2, 3, 0, 1)); /* v5v6 */

        /* (v8, v9, v10, v11) -> (v10, v11, v8, v9) */
        tmp1 = c0;
        c0 = c1;
        c1 = tmp1;

        /* (v12, v13, v14, v15) -> (v15, v12, v13, v14) */
        tmp1 = _mm256_blend_epi32(d0, d1, 0b11001100);                /* v12v15 */
        tmp2 = _mm256_blend_epi32(d0, d1, 0b00110011);                /* v14v13 */
        d0 = _mm256_permute4x64_epi64(tmp1, _MM_SHUFFLE(2, 3, 0, 1)); /* v15v12 */
        d1 = _mm256_permute4x64_epi64(tmp2, _MM_SHUFFLE(2, 3, 0, 1)); /* v13v14 */
    }

    static inline void UndiagonalizeAVX21(
        __m256i& b0, __m256i& c0, __m256i& d0, __m256i& b1, __m256i& c1, __m256i& d1) {
        /* (v5, v6, v7, v4) -> (v4, v5, v6, v7) */
        b0 = _mm256_permute4x64_epi64(b0, _MM_SHUFFLE(2, 1, 0, 3));
        /* (v10, v11, v8, v9) -> (v8, v9, v10, v11) */
        c0 = _mm256_permute4x64_epi64(c0, _MM_SHUFFLE(1, 0, 3, 2));
        /* (v15, v12, v13, v14) -> (v12, v13, v14, v15) */
        d0 = _mm256_permute4x64_epi64(d0, _MM_SHUFFLE(0, 3, 2, 1));

        b1 = _mm256_permute4x64_epi64(b1, _MM_SHUFFLE(2, 1, 0, 3));
        c1 = _mm256_permute4x64_epi64(c1, _MM_SHUFFLE(1, 0, 3, 2));
        d1 = _mm256_permute4x64_epi64(d1, _MM_SHUFFLE(0, 3, 2, 1));
    }

    static inline void UndiagonalizeAVX22(
        __m256i& b0, __m256i& b1, __m256i& c0, __m256i& c1, __m256i& d0, __m256i& d1) {
        /* (v5, v6, v7, v4) -> (v4, v5, v6, v7) */
        __m256i tmp1 = _mm256_blend_epi32(b0, b1, 0b11001100);        /* v5v4 */
        __m256i tmp2 = _mm256_blend_epi32(b0, b1, 0b00110011);        /* v7v6 */
        b0 = _mm256_permute4x64_epi64(tmp1, _MM_SHUFFLE(2, 3, 0, 1)); /* v4v5 */
        b1 = _mm256_permute4x64_epi64(tmp2, _MM_SHUFFLE(2, 3, 0, 1)); /* v6v7 */

        /* (v10,v11,v8,v9) -> (v8,v9,v10,v11) */
        tmp1 = c0;
        c0 = c1;
        c1 = tmp1;

        /* (v15,v12,v13,v14) -> (v12,v13,v14,v15) */
        tmp1 = _mm256_blend_epi32(d0, d1, 0b00110011); /* v13v12 */
        tmp2 = _mm256_blend_epi32(d0, d1, 0b11001100); /* v15v14 */
        d0 = _mm256_permute4x64_epi64(tmp1, _MM_SHUFFLE(2, 3, 0, 1));
        d1 = _mm256_permute4x64_epi64(tmp2, _MM_SHUFFLE(2, 3, 0, 1));
    }
}
