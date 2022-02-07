#pragma once

#include "blake2b.h"
#include <library/cpp/digest/argonish/internal/rotations/rotations_ref.h>

namespace NArgonish {
    static const ui8 Sigma[12][16] = {
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

    static const ui64 Iv[8] = {
        0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL,
        0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
        0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
        0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL};

    static inline void GRef(ui64 r, ui64 i, ui64& a, ui64& b, ui64& c, ui64& d, const ui64* m) {
        a = a + b + m[Sigma[r][2 * i + 0]];
        d = Rotr(d ^ a, 32);
        c = c + d;
        b = Rotr(b ^ c, 24);
        a = a + b + m[Sigma[r][2 * i + 1]];
        d = Rotr(d ^ a, 16);
        c = c + d;
        b = Rotr(b ^ c, 63);
    }

    static inline void Round(ui64 r, ui64* v, const ui64* m) {
        GRef(r, 0, v[0], v[4], v[8], v[12], m);
        GRef(r, 1, v[1], v[5], v[9], v[13], m);
        GRef(r, 2, v[2], v[6], v[10], v[14], m);
        GRef(r, 3, v[3], v[7], v[11], v[15], m);
        GRef(r, 4, v[0], v[5], v[10], v[15], m);
        GRef(r, 5, v[1], v[6], v[11], v[12], m);
        GRef(r, 6, v[2], v[7], v[8], v[13], m);
        GRef(r, 7, v[3], v[4], v[9], v[14], m);
    }

    template <>
    void* TBlake2B<EInstructionSet::REF>::GetIV_() const {
        return nullptr;
    }

    template <>
    void TBlake2B<EInstructionSet::REF>::InitialXor_(ui8* h, const ui8* p) {
        for (size_t i = 0; i < 8; ++i)
            ((ui64*)h)[i] = Iv[i] ^ ((ui64*)p)[i];
    }

    template <>
    void TBlake2B<EInstructionSet::REF>::Compress_(const ui64 block[BLAKE2B_BLOCKQWORDS]) {
        ui64 v[16];
        for (size_t i = 0; i < 8; ++i) {
            v[i] = State_.H[i];
        }

        v[8] = Iv[0];
        v[9] = Iv[1];
        v[10] = Iv[2];
        v[11] = Iv[3];
        v[12] = Iv[4] ^ State_.T[0];
        v[13] = Iv[5] ^ State_.T[1];
        v[14] = Iv[6] ^ State_.F[0];
        v[15] = Iv[7] ^ State_.F[1];

        for (ui64 r = 0; r < 12; ++r)
            Round(r, v, block);

        for (size_t i = 0; i < 8; ++i) {
            State_.H[i] = State_.H[i] ^ v[i] ^ v[i + 8];
        }
    }
}
