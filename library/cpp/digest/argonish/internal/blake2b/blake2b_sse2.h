#pragma once

#include <emmintrin.h>
#include "blake2b.h"
#include <library/cpp/digest/argonish/internal/rotations/rotations_sse2.h>

namespace NArgonish {
    template <>
    void* TBlake2B<EInstructionSet::SSE2>::GetIV_() const {
        static const __m128i Iv[4] = {
            _mm_set_epi64x(0xbb67ae8584caa73bULL, 0x6a09e667f3bcc908ULL),
            _mm_set_epi64x(0xa54ff53a5f1d36f1ULL, 0x3c6ef372fe94f82bULL),
            _mm_set_epi64x(0x9b05688c2b3e6c1fULL, 0x510e527fade682d1ULL),
            _mm_set_epi64x(0x5be0cd19137e2179ULL, 0x1f83d9abfb41bd6bULL)};

        return (void*)Iv;
    }

    static const ui32 Sigma[12][16] = {
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
        {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
        {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
        {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
        {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
        {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
        {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
        {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
        {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
        {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
        {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3}};

    static inline void G1(
        __m128i& row1l, __m128i& row2l, __m128i& row3l, __m128i& row4l,
        __m128i& row1h, __m128i& row2h, __m128i& row3h, __m128i& row4h,
        __m128i& b0, __m128i& b1) {
        row1l = _mm_add_epi64(_mm_add_epi64(row1l, b0), row2l);
        row1h = _mm_add_epi64(_mm_add_epi64(row1h, b1), row2h);

        row4l = _mm_xor_si128(row4l, row1l);
        row4h = _mm_xor_si128(row4h, row1h);

        row4l = Rotr32(row4l);
        row4h = Rotr32(row4h);

        row3l = _mm_add_epi64(row3l, row4l);
        row3h = _mm_add_epi64(row3h, row4h);

        row2l = _mm_xor_si128(row2l, row3l);
        row2h = _mm_xor_si128(row2h, row3h);

        row2l = Rotr24(row2l);
        row2h = Rotr24(row2h);
    }

    static inline void G2(
        __m128i& row1l, __m128i& row2l, __m128i& row3l, __m128i& row4l,
        __m128i& row1h, __m128i& row2h, __m128i& row3h, __m128i& row4h,
        __m128i& b0, __m128i& b1) {
        row1l = _mm_add_epi64(_mm_add_epi64(row1l, b0), row2l);
        row1h = _mm_add_epi64(_mm_add_epi64(row1h, b1), row2h);

        row4l = _mm_xor_si128(row4l, row1l);
        row4h = _mm_xor_si128(row4h, row1h);

        row4l = Rotr16(row4l);
        row4h = Rotr16(row4h);

        row3l = _mm_add_epi64(row3l, row4l);
        row3h = _mm_add_epi64(row3h, row4h);

        row2l = _mm_xor_si128(row2l, row3l);
        row2h = _mm_xor_si128(row2h, row3h);

        row2l = Rotr63(row2l);
        row2h = Rotr63(row2h);
    }

    static inline void Diagonalize(
        __m128i& row2l, __m128i& row3l, __m128i& row4l,
        __m128i& row2h, __m128i& row3h, __m128i& row4h) {
        __m128i t0 = row4l;
        __m128i t1 = row2l;
        row4l = row3l;
        row3l = row3h;
        row3h = row4l;
        row4l = _mm_unpackhi_epi64(row4h, _mm_unpacklo_epi64(t0, t0));
        row4h = _mm_unpackhi_epi64(t0, _mm_unpacklo_epi64(row4h, row4h));
        row2l = _mm_unpackhi_epi64(row2l, _mm_unpacklo_epi64(row2h, row2h));
        row2h = _mm_unpackhi_epi64(row2h, _mm_unpacklo_epi64(t1, t1));
    }

    static inline void Undiagonalize(
        __m128i& row2l, __m128i& row3l, __m128i& row4l,
        __m128i& row2h, __m128i& row3h, __m128i& row4h) {
        __m128i t0 = row3l;
        row3l = row3h;
        row3h = t0;
        t0 = row2l;
        __m128i t1 = row4l;
        row2l = _mm_unpackhi_epi64(row2h, _mm_unpacklo_epi64(row2l, row2l));
        row2h = _mm_unpackhi_epi64(t0, _mm_unpacklo_epi64(row2h, row2h));
        row4l = _mm_unpackhi_epi64(row4l, _mm_unpacklo_epi64(row4h, row4h));
        row4h = _mm_unpackhi_epi64(row4h, _mm_unpacklo_epi64(t1, t1));
    }

    static inline void Round(int r, const ui64* block_ptr,
                             __m128i& row1l, __m128i& row2l, __m128i& row3l, __m128i& row4l,
                             __m128i& row1h, __m128i& row2h, __m128i& row3h, __m128i& row4h) {
        __m128i b0, b1;
        b0 = _mm_set_epi64x(block_ptr[Sigma[r][2]], block_ptr[Sigma[r][0]]);
        b1 = _mm_set_epi64x(block_ptr[Sigma[r][6]], block_ptr[Sigma[r][4]]);
        G1(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1);
        b0 = _mm_set_epi64x(block_ptr[Sigma[r][3]], block_ptr[Sigma[r][1]]);
        b1 = _mm_set_epi64x(block_ptr[Sigma[r][7]], block_ptr[Sigma[r][5]]);
        G2(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1);
        Diagonalize(row2l, row3l, row4l, row2h, row3h, row4h);
        b0 = _mm_set_epi64x(block_ptr[Sigma[r][10]], block_ptr[Sigma[r][8]]);
        b1 = _mm_set_epi64x(block_ptr[Sigma[r][14]], block_ptr[Sigma[r][12]]);
        G1(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1);
        b0 = _mm_set_epi64x(block_ptr[Sigma[r][11]], block_ptr[Sigma[r][9]]);
        b1 = _mm_set_epi64x(block_ptr[Sigma[r][15]], block_ptr[Sigma[r][13]]);
        G2(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1);
        Undiagonalize(row2l, row3l, row4l, row2h, row3h, row4h);
    }

    template <>
    void TBlake2B<EInstructionSet::SSE2>::InitialXor_(ui8* h, const ui8* p) {
        __m128i* m_res = (__m128i*)h;
        const __m128i* m_p = (__m128i*)p;
        __m128i* iv = (__m128i*)GetIV_();

        _mm_storeu_si128(m_res + 0, _mm_xor_si128(iv[0], _mm_loadu_si128(m_p + 0)));
        _mm_storeu_si128(m_res + 1, _mm_xor_si128(iv[1], _mm_loadu_si128(m_p + 1)));
        _mm_storeu_si128(m_res + 2, _mm_xor_si128(iv[2], _mm_loadu_si128(m_p + 2)));
        _mm_storeu_si128(m_res + 3, _mm_xor_si128(iv[3], _mm_loadu_si128(m_p + 3)));
    }

    template <>
    void TBlake2B<EInstructionSet::SSE2>::Compress_(const ui64 block[BLAKE2B_BLOCKQWORDS]) {
        __m128i* iv = (__m128i*)GetIV_();
        __m128i row1l = _mm_loadu_si128((__m128i*)&State_.H[0]);
        __m128i row1h = _mm_loadu_si128((__m128i*)&State_.H[2]);
        __m128i row2l = _mm_loadu_si128((__m128i*)&State_.H[4]);
        __m128i row2h = _mm_loadu_si128((__m128i*)&State_.H[6]);
        __m128i row3l = iv[0];
        __m128i row3h = iv[1];
        __m128i row4l = _mm_xor_si128(iv[2], _mm_loadu_si128((__m128i*)&State_.T[0]));
        __m128i row4h = _mm_xor_si128(iv[3], _mm_loadu_si128((__m128i*)&State_.F[0]));

        for (int r = 0; r < 12; r++)
            Round(r, block, row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h);

        _mm_storeu_si128((__m128i*)&State_.H[0],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[0]), _mm_xor_si128(row3l, row1l)));
        _mm_storeu_si128((__m128i*)&State_.H[2],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[2]), _mm_xor_si128(row3h, row1h)));
        _mm_storeu_si128((__m128i*)&State_.H[4],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[4]), _mm_xor_si128(row4l, row2l)));
        _mm_storeu_si128((__m128i*)&State_.H[6],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[6]), _mm_xor_si128(row4h, row2h)));
    }
}
