#pragma once

#include <immintrin.h>
#include "blake2b.h"
#include <library/cpp/digest/argonish/internal/rotations/rotations_avx2.h>

namespace NArgonish {
    template <>
    void* TBlake2B<EInstructionSet::AVX2>::GetIV_() const {
        static const __m256i Iv[2] = {
            _mm256_set_epi64x(0xa54ff53a5f1d36f1ULL, 0x3c6ef372fe94f82bULL, 0xbb67ae8584caa73bULL, 0x6a09e667f3bcc908ULL),
            _mm256_set_epi64x(0x5be0cd19137e2179ULL, 0x1f83d9abfb41bd6bULL, 0x9b05688c2b3e6c1fULL, 0x510e527fade682d1ULL)};
        return (void*)Iv;
    }

    template <>
    void TBlake2B<EInstructionSet::AVX2>::InitialXor_(ui8* h, const ui8* p) {
        __m256i* iv = (__m256i*)GetIV_();
        __m256i* m_res = (__m256i*)h;
        const __m256i* m_second = (__m256i*)p;
        _mm256_storeu_si256(m_res, _mm256_xor_si256(iv[0], _mm256_loadu_si256(m_second)));
        _mm256_storeu_si256(m_res + 1, _mm256_xor_si256(iv[1], _mm256_loadu_si256(m_second + 1)));
    }

    /*
     * a =  v0,  v1,  v2,  v3
     * b =  v4,  v5,  v6,  v7
     * c =  v8,  v9, v10, v11
     * d = v12, v13, v14, v15
     */
    static inline void G1AVX2(ui32 r, __m256i& a, __m256i& b, __m256i& c, __m256i& d, const ui64* blk, const __m128i vindex[12][4]) {
        a = _mm256_add_epi64(a, _mm256_add_epi64(b, _mm256_i32gather_epi64((const long long int*)blk, vindex[r][0], 8)));
        d = Rotr32(_mm256_xor_si256(a, d));
        c = _mm256_add_epi64(c, d);
        b = Rotr24(_mm256_xor_si256(b, c));

        a = _mm256_add_epi64(a, _mm256_add_epi64(b, _mm256_i32gather_epi64((const long long int*)blk, vindex[r][1], 8)));
        d = Rotr16(_mm256_xor_si256(a, d));
        c = _mm256_add_epi64(c, d);
        b = Rotr63(_mm256_xor_si256(b, c));
    }

    static inline void G2AVX2(ui32 r, __m256i& a, __m256i& b, __m256i& c, __m256i& d, const ui64* blk, const __m128i vindex[12][4]) {
        a = _mm256_add_epi64(a, _mm256_add_epi64(b, _mm256_i32gather_epi64((const long long int*)blk, vindex[r][2], 8)));
        d = Rotr32(_mm256_xor_si256(a, d));
        c = _mm256_add_epi64(c, d);
        b = Rotr24(_mm256_xor_si256(b, c));

        a = _mm256_add_epi64(a, _mm256_add_epi64(b, _mm256_i32gather_epi64((const long long int*)blk, vindex[r][3], 8)));
        d = Rotr16(_mm256_xor_si256(a, d));
        c = _mm256_add_epi64(c, d);
        b = Rotr63(_mm256_xor_si256(b, c));
    }

    static inline void Diagonalize(__m256i& b, __m256i& c, __m256i& d) {
        b = _mm256_permute4x64_epi64(b, _MM_SHUFFLE(0, 3, 2, 1));
        c = _mm256_permute4x64_epi64(c, _MM_SHUFFLE(1, 0, 3, 2));
        d = _mm256_permute4x64_epi64(d, _MM_SHUFFLE(2, 1, 0, 3));
    }

    static inline void Undiagonalize(__m256i& b, __m256i& c, __m256i& d) {
        b = _mm256_permute4x64_epi64(b, _MM_SHUFFLE(2, 1, 0, 3));
        c = _mm256_permute4x64_epi64(c, _MM_SHUFFLE(1, 0, 3, 2));
        d = _mm256_permute4x64_epi64(d, _MM_SHUFFLE(0, 3, 2, 1));
    }

    template <>
    void TBlake2B<EInstructionSet::AVX2>::Compress_(const ui64 block[BLAKE2B_BLOCKQWORDS]) {
        static const __m128i VIndex[12][4] = {
            {_mm_set_epi32(6, 4, 2, 0), _mm_set_epi32(7, 5, 3, 1), _mm_set_epi32(14, 12, 10, 8), _mm_set_epi32(15, 13, 11, 9)},
            {_mm_set_epi32(13, 9, 4, 14), _mm_set_epi32(6, 15, 8, 10), _mm_set_epi32(5, 11, 0, 1), _mm_set_epi32(3, 7, 2, 12)},
            {_mm_set_epi32(15, 5, 12, 11), _mm_set_epi32(13, 2, 0, 8), _mm_set_epi32(9, 7, 3, 10), _mm_set_epi32(4, 1, 6, 14)},
            {_mm_set_epi32(11, 13, 3, 7), _mm_set_epi32(14, 12, 1, 9), _mm_set_epi32(15, 4, 5, 2), _mm_set_epi32(8, 0, 10, 6)},
            {_mm_set_epi32(10, 2, 5, 9), _mm_set_epi32(15, 4, 7, 0), _mm_set_epi32(3, 6, 11, 14), _mm_set_epi32(13, 8, 12, 1)},
            {_mm_set_epi32(8, 0, 6, 2), _mm_set_epi32(3, 11, 10, 12), _mm_set_epi32(1, 15, 7, 4), _mm_set_epi32(9, 14, 5, 13)},
            {_mm_set_epi32(4, 14, 1, 12), _mm_set_epi32(10, 13, 15, 5), _mm_set_epi32(8, 9, 6, 0), _mm_set_epi32(11, 2, 3, 7)},
            {_mm_set_epi32(3, 12, 7, 13), _mm_set_epi32(9, 1, 14, 11), _mm_set_epi32(2, 8, 15, 5), _mm_set_epi32(10, 6, 4, 0)},
            {_mm_set_epi32(0, 11, 14, 6), _mm_set_epi32(8, 3, 9, 15), _mm_set_epi32(10, 1, 13, 12), _mm_set_epi32(5, 4, 7, 2)},
            {_mm_set_epi32(1, 7, 8, 10), _mm_set_epi32(5, 6, 4, 2), _mm_set_epi32(13, 3, 9, 15), _mm_set_epi32(0, 12, 14, 11)},
            {_mm_set_epi32(6, 4, 2, 0), _mm_set_epi32(7, 5, 3, 1), _mm_set_epi32(14, 12, 10, 8), _mm_set_epi32(15, 13, 11, 9)},
            {_mm_set_epi32(13, 9, 4, 14), _mm_set_epi32(6, 15, 8, 10), _mm_set_epi32(5, 11, 0, 1), _mm_set_epi32(3, 7, 2, 12)},
        };

        __m256i* iv = (__m256i*)GetIV_();
        __m256i a = _mm256_loadu_si256((__m256i*)&State_.H[0]);
        __m256i b = _mm256_loadu_si256((__m256i*)&State_.H[4]);
        __m256i c = iv[0];
        __m256i d = _mm256_xor_si256(iv[1], _mm256_loadu_si256((__m256i*)&State_.T[0]));

        for (ui32 r = 0; r < 12; ++r) {
            G1AVX2(r, a, b, c, d, block, VIndex);
            Diagonalize(b, c, d);
            G2AVX2(r, a, b, c, d, block, VIndex);
            Undiagonalize(b, c, d);
        }

        _mm256_storeu_si256((__m256i*)State_.H, _mm256_xor_si256(
                                                    _mm256_loadu_si256((__m256i*)State_.H),
                                                    _mm256_xor_si256(a, c)));
        _mm256_storeu_si256(((__m256i*)State_.H) + 1, _mm256_xor_si256(
                                                          _mm256_loadu_si256(((__m256i*)State_.H) + 1),
                                                          _mm256_xor_si256(b, d)));
    }
}
