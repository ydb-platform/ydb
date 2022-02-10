#pragma once

#include <smmintrin.h>
#include "argon2_base.h"
#include <library/cpp/digest/argonish/internal/blamka/blamka_ssse3.h>

namespace NArgonish {
    template <ui32 mcost, ui32 threads>
    class TArgon2SSE41 final: public TArgon2<EInstructionSet::SSE41, mcost, threads> {
    public:
        TArgon2SSE41(EArgon2Type atype, ui32 tcost, const ui8* key, ui32 keylen)
            : TArgon2<EInstructionSet::SSE41, mcost, threads>(atype, tcost, key, keylen)
        {
        }

    protected:
        virtual void XorBlock_(TBlock* dst, const TBlock* src) const override {
            __m128i* mdst = (__m128i*)dst->V;
            __m128i* msrc = (__m128i*)src->V;

            for (ui32 i = 0; i < ARGON2_OWORDS_IN_BLOCK; ++i)
                XorValues(mdst + i, msrc + i, mdst + i);
        }

        virtual void CopyBlock_(TBlock* dst, const TBlock* src) const override {
            memcpy(dst->V, src->V, sizeof(ui64) * ARGON2_QWORDS_IN_BLOCK);
        }

        virtual void FillBlock_(const TBlock* prevBlock, const TBlock* refBlock, TBlock* nextBlock, bool withXor) const override {
            __m128i blockxy[ARGON2_OWORDS_IN_BLOCK];
            __m128i state[ARGON2_OWORDS_IN_BLOCK];

            memcpy(state, prevBlock, ARGON2_BLOCK_SIZE);

            if (withXor) {
                for (ui32 i = 0; i < ARGON2_OWORDS_IN_BLOCK; ++i) {
                    state[i] = _mm_xor_si128(
                        state[i], _mm_loadu_si128((const __m128i*)refBlock->V + i));
                    blockxy[i] = _mm_xor_si128(
                        state[i], _mm_loadu_si128((const __m128i*)nextBlock->V + i));
                }
            } else {
                for (ui32 i = 0; i < ARGON2_OWORDS_IN_BLOCK; ++i) {
                    blockxy[i] = state[i] = _mm_xor_si128(
                        state[i], _mm_loadu_si128((const __m128i*)refBlock->V + i));
                }
            }

            for (ui32 i = 0; i < 8; ++i) {
                BlamkaG1SSSE3(
                    state[8 * i + 0], state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
                BlamkaG2SSSE3(
                    state[8 * i + 0], state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
                DiagonalizeSSSE3(
                    state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5],
                    state[8 * i + 6], state[8 * i + 7]);
                BlamkaG1SSSE3(
                    state[8 * i + 0], state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
                BlamkaG2SSSE3(
                    state[8 * i + 0], state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
                UndiagonalizeSSSE3(
                    state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 4], state[8 * i + 5],
                    state[8 * i + 6], state[8 * i + 7]);
            }

            for (ui32 i = 0; i < 8; ++i) {
                BlamkaG1SSSE3(
                    state[8 * 0 + i], state[8 * 1 + i], state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i], state[8 * 6 + i], state[8 * 7 + i]);
                BlamkaG2SSSE3(
                    state[8 * 0 + i], state[8 * 1 + i], state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i], state[8 * 6 + i], state[8 * 7 + i]);
                DiagonalizeSSSE3(
                    state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i],
                    state[8 * 6 + i], state[8 * 7 + i]);
                BlamkaG1SSSE3(
                    state[8 * 0 + i], state[8 * 1 + i], state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i], state[8 * 6 + i], state[8 * 7 + i]);
                BlamkaG2SSSE3(
                    state[8 * 0 + i], state[8 * 1 + i], state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i], state[8 * 6 + i], state[8 * 7 + i]);
                UndiagonalizeSSSE3(
                    state[8 * 2 + i], state[8 * 3 + i],
                    state[8 * 4 + i], state[8 * 5 + i],
                    state[8 * 6 + i], state[8 * 7 + i]);
            }

            for (ui32 i = 0; i < ARGON2_OWORDS_IN_BLOCK; ++i) {
                state[i] = _mm_xor_si128(state[i], blockxy[i]);
                _mm_storeu_si128((__m128i*)nextBlock->V + i, state[i]);
            }
        }
    };
}
