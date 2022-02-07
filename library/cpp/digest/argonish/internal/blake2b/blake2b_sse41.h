#pragma once

#include <smmintrin.h>
#include "blake2b.h"
#include "load_sse41.h"
#include <library/cpp/digest/argonish/internal/rotations/rotations_ssse3.h>

namespace NArgonish {
    template <>
    void* TBlake2B<EInstructionSet::SSE41>::GetIV_() const {
        static const __m128i Iv[4] = {
            _mm_set_epi64x(0xbb67ae8584caa73bULL, 0x6a09e667f3bcc908ULL),
            _mm_set_epi64x(0xa54ff53a5f1d36f1ULL, 0x3c6ef372fe94f82bULL),
            _mm_set_epi64x(0x9b05688c2b3e6c1fULL, 0x510e527fade682d1ULL),
            _mm_set_epi64x(0x5be0cd19137e2179ULL, 0x1f83d9abfb41bd6bULL)};
        return (void*)Iv;
    }

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
        __m128i t0 = _mm_alignr_epi8(row2h, row2l, 8);
        __m128i t1 = _mm_alignr_epi8(row2l, row2h, 8);
        row2l = t0;
        row2h = t1;

        t0 = row3l;
        row3l = row3h;
        row3h = t0;

        t0 = _mm_alignr_epi8(row4h, row4l, 8);
        t1 = _mm_alignr_epi8(row4l, row4h, 8);
        row4l = t1;
        row4h = t0;
    }

    static inline void Undiagonalize(
        __m128i& row2l, __m128i& row3l, __m128i& row4l,
        __m128i& row2h, __m128i& row3h, __m128i& row4h) {
        __m128i t0 = _mm_alignr_epi8(row2l, row2h, 8);
        __m128i t1 = _mm_alignr_epi8(row2h, row2l, 8);
        row2l = t0;
        row2h = t1;

        t0 = row3l;
        row3l = row3h;
        row3h = t0;

        t0 = _mm_alignr_epi8(row4l, row4h, 8);
        t1 = _mm_alignr_epi8(row4h, row4l, 8);
        row4l = t1;
        row4h = t0;
    }

#define ROUND(r)                                                        \
    LOAD_MSG_##r##_1(b0, b1);                                           \
    G1(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1); \
    LOAD_MSG_##r##_2(b0, b1);                                           \
    G2(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1); \
    Diagonalize(row2l, row3l, row4l, row2h, row3h, row4h);              \
    LOAD_MSG_##r##_3(b0, b1);                                           \
    G1(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1); \
    LOAD_MSG_##r##_4(b0, b1);                                           \
    G2(row1l, row2l, row3l, row4l, row1h, row2h, row3h, row4h, b0, b1); \
    Undiagonalize(row2l, row3l, row4l, row2h, row3h, row4h);

    template <>
    void TBlake2B<EInstructionSet::SSE41>::InitialXor_(ui8* h, const ui8* p) {
        __m128i* m_res = (__m128i*)h;
        const __m128i* m_p = (__m128i*)p;
        __m128i* iv = (__m128i*)GetIV_();

        _mm_storeu_si128(m_res + 0, _mm_xor_si128(iv[0], _mm_loadu_si128(m_p + 0)));
        _mm_storeu_si128(m_res + 1, _mm_xor_si128(iv[1], _mm_loadu_si128(m_p + 1)));
        _mm_storeu_si128(m_res + 2, _mm_xor_si128(iv[2], _mm_loadu_si128(m_p + 2)));
        _mm_storeu_si128(m_res + 3, _mm_xor_si128(iv[3], _mm_loadu_si128(m_p + 3)));
    }

    template <>
    void TBlake2B<EInstructionSet::SSE41>::Compress_(const ui64 block[BLAKE2B_BLOCKQWORDS]) {
        const __m128i* block_ptr = (__m128i*)block;
        __m128i* iv = (__m128i*)GetIV_();
        const __m128i m0 = _mm_loadu_si128(block_ptr + 0);
        const __m128i m1 = _mm_loadu_si128(block_ptr + 1);
        const __m128i m2 = _mm_loadu_si128(block_ptr + 2);
        const __m128i m3 = _mm_loadu_si128(block_ptr + 3);
        const __m128i m4 = _mm_loadu_si128(block_ptr + 4);
        const __m128i m5 = _mm_loadu_si128(block_ptr + 5);
        const __m128i m6 = _mm_loadu_si128(block_ptr + 6);
        const __m128i m7 = _mm_loadu_si128(block_ptr + 7);

        __m128i row1l = _mm_loadu_si128((__m128i*)&State_.H[0]);
        __m128i row1h = _mm_loadu_si128((__m128i*)&State_.H[2]);
        __m128i row2l = _mm_loadu_si128((__m128i*)&State_.H[4]);
        __m128i row2h = _mm_loadu_si128((__m128i*)&State_.H[6]);
        __m128i row3l = iv[0];
        __m128i row3h = iv[1];
        __m128i row4l = _mm_xor_si128(iv[2], _mm_loadu_si128((__m128i*)&State_.T[0]));
        __m128i row4h = _mm_xor_si128(iv[3], _mm_loadu_si128((__m128i*)&State_.F[0]));
        __m128i b0, b1;

        ROUND(0);
        ROUND(1);
        ROUND(2);
        ROUND(3);
        ROUND(4);
        ROUND(5);
        ROUND(6);
        ROUND(7);
        ROUND(8);
        ROUND(9);
        ROUND(10);
        ROUND(11);

        _mm_storeu_si128((__m128i*)&State_.H[0],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[0]), _mm_xor_si128(row3l, row1l)));
        _mm_storeu_si128((__m128i*)&State_.H[2],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[2]), _mm_xor_si128(row3h, row1h)));
        _mm_storeu_si128((__m128i*)&State_.H[4],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[4]), _mm_xor_si128(row4l, row2l)));
        _mm_storeu_si128((__m128i*)&State_.H[6],
                         _mm_xor_si128(_mm_loadu_si128((__m128i*)&State_.H[6]), _mm_xor_si128(row4h, row2h)));
    }

#undef ROUND
}
