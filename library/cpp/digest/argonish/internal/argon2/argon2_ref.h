#pragma once

#include "argon2_base.h"
#include <library/cpp/digest/argonish/internal/rotations/rotations_ref.h>

namespace NArgonish {
    static inline ui64 FBlaMka(ui64 x, ui64 y) {
        const ui64 m = 0xFFFFFFFF;
        const ui64 xy = (x & m) * (y & m);
        return x + y + 2 * xy;
    }

    static inline void BlamkaGRef(ui64& a, ui64& b, ui64& c, ui64& d) {
        a = FBlaMka(a, b);
        d = Rotr(d ^ a, 32);
        c = FBlaMka(c, d);
        b = Rotr(b ^ c, 24);
        a = FBlaMka(a, b);
        d = Rotr(d ^ a, 16);
        c = FBlaMka(c, d);
        b = Rotr(b ^ c, 63);
    }

    static inline void BlamkaRoundRef(
        ui64& v0, ui64& v1, ui64& v2, ui64& v3,
        ui64& v4, ui64& v5, ui64& v6, ui64& v7,
        ui64& v8, ui64& v9, ui64& v10, ui64& v11,
        ui64& v12, ui64& v13, ui64& v14, ui64& v15) {
        BlamkaGRef(v0, v4, v8, v12);
        BlamkaGRef(v1, v5, v9, v13);
        BlamkaGRef(v2, v6, v10, v14);
        BlamkaGRef(v3, v7, v11, v15);
        BlamkaGRef(v0, v5, v10, v15);
        BlamkaGRef(v1, v6, v11, v12);
        BlamkaGRef(v2, v7, v8, v13);
        BlamkaGRef(v3, v4, v9, v14);
    }

    template <ui32 mcost, ui32 threads>
    class TArgon2REF final: public TArgon2<EInstructionSet::REF, mcost, threads> {
    public:
        TArgon2REF(EArgon2Type atype, ui32 tcost, const ui8* key, ui32 keylen)
            : TArgon2<EInstructionSet::REF, mcost, threads>(atype, tcost, key, keylen)
        {
        }

    protected:
        virtual void XorBlock_(TBlock* dst, const TBlock* src) const override {
            for (ui32 i = 0; i < ARGON2_QWORDS_IN_BLOCK; ++i) {
                dst->V[i] ^= src->V[i];
            }
        }

        virtual void CopyBlock_(TBlock* dst, const TBlock* src) const override {
            memcpy(dst->V, src->V, sizeof(ui64) * ARGON2_QWORDS_IN_BLOCK);
        }

        virtual void FillBlock_(const TBlock* prevBlock, const TBlock* refBlock, TBlock* nextBlock, bool withXor) const override {
            TBlock blockR, blockTmp;
            CopyBlock_(&blockR, refBlock);
            XorBlock_(&blockR, prevBlock);
            CopyBlock_(&blockTmp, &blockR);

            if (withXor) {
                XorBlock_(&blockTmp, nextBlock);
            }

            for (ui32 i = 0; i < 8; ++i) {
                BlamkaRoundRef(
                    blockR.V[16 * i + 0], blockR.V[16 * i + 1], blockR.V[16 * i + 2], blockR.V[16 * i + 3],
                    blockR.V[16 * i + 4], blockR.V[16 * i + 5], blockR.V[16 * i + 6], blockR.V[16 * i + 7],
                    blockR.V[16 * i + 8], blockR.V[16 * i + 9], blockR.V[16 * i + 10], blockR.V[16 * i + 11],
                    blockR.V[16 * i + 12], blockR.V[16 * i + 13], blockR.V[16 * i + 14], blockR.V[16 * i + 15]);
            }

            for (ui32 i = 0; i < 8; ++i) {
                BlamkaRoundRef(
                    blockR.V[2 * i + 0], blockR.V[2 * i + 1], blockR.V[2 * i + 16], blockR.V[2 * i + 17],
                    blockR.V[2 * i + 32], blockR.V[2 * i + 33], blockR.V[2 * i + 48], blockR.V[2 * i + 49],
                    blockR.V[2 * i + 64], blockR.V[2 * i + 65], blockR.V[2 * i + 80], blockR.V[2 * i + 81],
                    blockR.V[2 * i + 96], blockR.V[2 * i + 97], blockR.V[2 * i + 112], blockR.V[2 * i + 113]);
            }

            CopyBlock_(nextBlock, &blockTmp);
            XorBlock_(nextBlock, &blockR);
        }
    };
}
