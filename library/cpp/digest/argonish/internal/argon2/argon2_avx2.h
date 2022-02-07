#pragma once

#include <immintrin.h>
#include "argon2_base.h"
#include <library/cpp/digest/argonish/internal/blamka/blamka_avx2.h>

namespace NArgonish {
    template <ui32 mcost, ui32 threads>
    class TArgon2AVX2 final: public TArgon2<EInstructionSet::AVX2, mcost, threads> {
    public:
        TArgon2AVX2(EArgon2Type atype, ui32 tcost, const ui8* key, ui32 keylen)
            : TArgon2<EInstructionSet::AVX2, mcost, threads>(atype, tcost, key, keylen)
        {
        }

    protected:
        virtual void XorBlock_(TBlock* dst, const TBlock* src) const override {
            __m256i* mdst = (__m256i*)dst;
            __m256i* msrc = (__m256i*)src;

            for (ui32 i = 0; i < ARGON2_HWORDS_IN_BLOCK; ++i)
                XorValues(mdst + i, mdst + i, msrc + i);
        }

        virtual void CopyBlock_(TBlock* dst, const TBlock* src) const override {
            memcpy(dst->V, src->V, sizeof(ui64) * ARGON2_QWORDS_IN_BLOCK);
        }

        virtual void FillBlock_(const TBlock* prevBlock, const TBlock* refBlock, TBlock* nextBlock, bool with_xor) const override {
            __m256i blockxy[ARGON2_HWORDS_IN_BLOCK];
            __m256i state[ARGON2_HWORDS_IN_BLOCK];

            memcpy(state, prevBlock, ARGON2_BLOCK_SIZE);

            if (with_xor) {
                for (ui32 i = 0; i < ARGON2_HWORDS_IN_BLOCK; ++i) {
                    state[i] = _mm256_xor_si256(state[i], _mm256_loadu_si256((const __m256i*)refBlock->V + i));
                    blockxy[i] = _mm256_xor_si256(state[i], _mm256_loadu_si256((const __m256i*)nextBlock->V + i));
                }
            } else {
                for (ui32 i = 0; i < ARGON2_HWORDS_IN_BLOCK; ++i) {
                    blockxy[i] = state[i] = _mm256_xor_si256(
                        state[i], _mm256_loadu_si256((const __m256i*)refBlock->V + i));
                }
            }

            /**
             * state[ 8*i + 0 ] = ( v0_0,  v1_0,  v2_0,  v3_0)
             * state[ 8*i + 1 ] = ( v4_0,  v5_0,  v6_0,  v7_0)
             * state[ 8*i + 2 ] = ( v8_0,  v9_0, v10_0, v11_0)
             * state[ 8*i + 3 ] = (v12_0, v13_0, v14_0, v15_0)
             * state[ 8*i + 4 ] = ( v0_1,  v1_1,  v2_1,  v3_1)
             * state[ 8*i + 5 ] = ( v4_1,  v5_1,  v6_1,  v7_1)
             * state[ 8*i + 6 ] = ( v8_1,  v9_1, v10_1, v11_1)
             * state[ 8*i + 7 ] = (v12_1, v13_1, v14_1, v15_1)
             */
            for (ui32 i = 0; i < 4; ++i) {
                BlamkaG1AVX2(
                    state[8 * i + 0], state[8 * i + 4], state[8 * i + 1], state[8 * i + 5],
                    state[8 * i + 2], state[8 * i + 6], state[8 * i + 3], state[8 * i + 7]);
                BlamkaG2AVX2(
                    state[8 * i + 0], state[8 * i + 4], state[8 * i + 1], state[8 * i + 5],
                    state[8 * i + 2], state[8 * i + 6], state[8 * i + 3], state[8 * i + 7]);
                DiagonalizeAVX21(
                    state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
                BlamkaG1AVX2(
                    state[8 * i + 0], state[8 * i + 4], state[8 * i + 1], state[8 * i + 5],
                    state[8 * i + 2], state[8 * i + 6], state[8 * i + 3], state[8 * i + 7]);
                BlamkaG2AVX2(
                    state[8 * i + 0], state[8 * i + 4], state[8 * i + 1], state[8 * i + 5],
                    state[8 * i + 2], state[8 * i + 6], state[8 * i + 3], state[8 * i + 7]);
                UndiagonalizeAVX21(
                    state[8 * i + 1], state[8 * i + 2], state[8 * i + 3],
                    state[8 * i + 5], state[8 * i + 6], state[8 * i + 7]);
            }

            /**
             * state[ 0 + i] = ( v0_0,  v1_0,  v0_1,  v1_1)
             * state[ 4 + i] = ( v2_0,  v3_0,  v2_1,  v3_1)
             * state[ 8 + i] = ( v4_0,  v5_0,  v4_1,  v5_1)
             * state[12 + i] = ( v6_0,  v7_0,  v6_1,  v7_1)
             * state[16 + i] = ( v8_0,  v9_0,  v8_1,  v9_1)
             * state[20 + i] = (v10_0, v11_0, v10_1, v11_1)
             * state[24 + i] = (v12_0, v13_0, v12_1, v13_1)
             * state[28 + i] = (v14_0, v15_0, v14_1, v15_1)
             */
            for (ui32 i = 0; i < 4; ++i) {
                BlamkaG1AVX2(
                    state[0 + i], state[4 + i], state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i], state[24 + i], state[28 + i]);
                BlamkaG2AVX2(
                    state[0 + i], state[4 + i], state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i], state[24 + i], state[28 + i]);
                DiagonalizeAVX22(
                    state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i],
                    state[24 + i], state[28 + i]);
                BlamkaG1AVX2(
                    state[0 + i], state[4 + i], state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i], state[24 + i], state[28 + i]);
                BlamkaG2AVX2(
                    state[0 + i], state[4 + i], state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i], state[24 + i], state[28 + i]);
                UndiagonalizeAVX22(
                    state[8 + i], state[12 + i],
                    state[16 + i], state[20 + i],
                    state[24 + i], state[28 + i]);
            }

            for (ui32 i = 0; i < ARGON2_HWORDS_IN_BLOCK; ++i) {
                state[i] = _mm256_xor_si256(state[i], blockxy[i]);
                _mm256_storeu_si256((__m256i*)nextBlock->V + i, state[i]);
            }
        }
    };
}
