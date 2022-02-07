#pragma once

#include <emmintrin.h>

namespace NArgonish {
    static inline void XorValues(__m128i* result, const __m128i* val1, const __m128i* val2) {
        _mm_storeu_si128(result, _mm_xor_si128(
                                     _mm_loadu_si128(val1),
                                     _mm_loadu_si128(val2)));
    }

    static inline __m128i Rotr32(__m128i x) {
        return _mm_shuffle_epi32(x, _MM_SHUFFLE(2, 3, 0, 1));
    }

    static inline __m128i Rotr24(__m128i x) {
        return _mm_xor_si128(_mm_srli_epi64(x, 24), _mm_slli_epi64(x, 40));
    }

    static inline __m128i Rotr16(__m128i x) {
        return _mm_xor_si128(_mm_srli_epi64(x, 16), _mm_slli_epi64(x, 48));
    }

    static inline __m128i Rotr63(__m128i x) {
        return _mm_xor_si128(_mm_srli_epi64(x, 63), _mm_add_epi64(x, x));
    }
}
