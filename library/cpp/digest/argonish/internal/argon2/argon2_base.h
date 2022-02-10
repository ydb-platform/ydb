#pragma once

#include <util/generic/yexception.h>
#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/digest/argonish/internal/blake2b/blake2b.h>
#include <library/cpp/threading/poor_man_openmp/thread_helper.h>

namespace NArgonish {
    const ui32 ARGON2_PREHASH_DIGEST_LENGTH = 64;
    const ui32 ARGON2_SECRET_MAX_LENGTH = 64;
    const ui32 ARGON2_PREHASH_SEED_LENGTH = 72;
    const ui32 ARGON2_BLOCK_SIZE = 1024;
    const ui32 ARGON2_QWORDS_IN_BLOCK = ARGON2_BLOCK_SIZE / 8;
    const ui32 ARGON2_OWORDS_IN_BLOCK = ARGON2_BLOCK_SIZE / 16;
    const ui32 ARGON2_HWORDS_IN_BLOCK = ARGON2_BLOCK_SIZE / 32;
    const ui32 ARGON2_ADDRESSES_IN_BLOCK = 128;
    const ui32 ARGON2_SYNC_POINTS = 4;
    const ui32 ARGON2_SALT_MIN_LEN = 8;
    const ui32 ARGON2_MIN_OUTLEN = 4;

    struct TBlock {
        ui64 V[ARGON2_QWORDS_IN_BLOCK];
    };

    template <EInstructionSet instructionSet, ui32 mcost, ui32 threads>
    class TArgon2: public IArgon2Base {
    public:
        TArgon2(EArgon2Type atype, ui32 tcost, const ui8* key, ui32 keylen)
            : SecretLen_(keylen)
            , Tcost_(tcost)
            , Atype_(atype)
        {
            if (SecretLen_)
                memcpy(Secret_, key, keylen);
        }

        virtual ~TArgon2() override {
            if (SecretLen_) {
                SecureZeroMemory_(Secret_, SecretLen_);
                SecretLen_ = 0;
            }
        }

        virtual void Hash(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,
                          ui8* out, ui32 outlen, const ui8* aad = nullptr, ui32 aadlen = 0) const override {
            TArrayHolder<TBlock> buffer(new TBlock[MemoryBlocks_]);
            InternalHash_(buffer.Get(), pwd, pwdlen, salt, saltlen, out, outlen, aad, aadlen);
        }

        virtual bool Verify(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,
                            const ui8* hash, ui32 hashlen, const ui8* aad = nullptr, ui32 aadlen = 0) const override {
            TArrayHolder<ui8> hashResult(new ui8[hashlen]);
            Hash(pwd, pwdlen, salt, saltlen, hashResult.Get(), hashlen, aad, aadlen);

            return SecureCompare_(hash, hashResult.Get(), hashlen);
        }

        virtual void HashWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,
                                          const ui8* salt, ui32 saltlen, ui8* out, ui32 outlen,
                                          const ui8* aad = nullptr, ui32 aadlen = 0) const override {
            if (memory == nullptr || mlen < sizeof(TBlock) * MemoryBlocks_)
                ythrow yexception() << "memory is null or its size is not enough";

            InternalHash_((TBlock*)memory, pwd, pwdlen, salt, saltlen, out, outlen, aad, aadlen);
        }

        virtual bool VerifyWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,
                                            const ui8* salt, ui32 saltlen, const ui8* hash, ui32 hashlen,
                                            const ui8* aad = nullptr, ui32 aadlen = 0) const override {
            TArrayHolder<ui8> hashResult(new ui8[hashlen]);
            HashWithCustomMemory(memory, mlen, pwd, pwdlen, salt, saltlen, hashResult.Get(), hashlen, aad, aadlen);

            return SecureCompare_(hashResult.Get(), hash, hashlen);
        }

        virtual size_t GetMemorySize() const override {
            return MemoryBlocks_ * sizeof(TBlock);
        }

    protected: /* Constants */
        ui8 Secret_[ARGON2_SECRET_MAX_LENGTH] = {0};
        ui32 SecretLen_ = 0;
        ui32 Tcost_;
        EArgon2Type Atype_;

        static constexpr ui32 Lanes_ = threads;
        static constexpr ui32 MemoryBlocks_ = (mcost >= 2 * ARGON2_SYNC_POINTS * Lanes_) ? (mcost - mcost % (Lanes_ * ARGON2_SYNC_POINTS)) : 2 * ARGON2_SYNC_POINTS * Lanes_;
        static constexpr ui32 SegmentLength_ = MemoryBlocks_ / (Lanes_ * ARGON2_SYNC_POINTS);
        static constexpr ui32 LaneLength_ = SegmentLength_ * ARGON2_SYNC_POINTS;

    protected: /* Prototypes */
        virtual void FillBlock_(const TBlock* prevBlock, const TBlock* refBlock,
                                TBlock* nextBlock, bool withXor) const = 0;

        virtual void CopyBlock_(TBlock* dst, const TBlock* src) const = 0;
        virtual void XorBlock_(TBlock* dst, const TBlock* src) const = 0;

    protected: /* Static functions */
        static bool SecureCompare_(const ui8* buffer1, const ui8* buffer2, ui32 len) {
            bool result = true;
            for (ui32 i = 0; i < len; ++i) {
                result &= (buffer1[i] == buffer2[i]);
            }
            return result;
        }

        static void SecureZeroMemory_(void* src, size_t len) {
            static void* (*const volatile memset_v)(void*, int, size_t) = &memset;
            memset_v(src, 0, len);
        }

        static void Store32_(ui32 value, void* mem) {
            *((ui32*)mem) = value;
        }

        static void Blake2BHash64_(ui8 out[BLAKE2B_OUTBYTES], const ui8 in[BLAKE2B_OUTBYTES]) {
            TBlake2B<instructionSet> hash(BLAKE2B_OUTBYTES);
            hash.Update(in, BLAKE2B_OUTBYTES);
            hash.Final(out, BLAKE2B_OUTBYTES);
        }

        static void ExpandBlockhash_(ui8 expanded[ARGON2_BLOCK_SIZE], const ui8 blockhash[ARGON2_PREHASH_SEED_LENGTH]) {
            ui8 out_buffer[BLAKE2B_OUTBYTES];
            ui8 in_buffer[BLAKE2B_OUTBYTES];
            const ui32 HALF_OUT_BYTES = BLAKE2B_OUTBYTES / 2;
            const ui32 HASH_BLOCKS_COUNT = ((ARGON2_BLOCK_SIZE / HALF_OUT_BYTES));

            TBlake2B<instructionSet> hash(BLAKE2B_OUTBYTES);
            hash.Update(ARGON2_BLOCK_SIZE);
            hash.Update(blockhash, ARGON2_PREHASH_SEED_LENGTH);
            hash.Final(out_buffer, BLAKE2B_OUTBYTES);

            memcpy(expanded, out_buffer, HALF_OUT_BYTES);

            for (ui32 i = 1; i < HASH_BLOCKS_COUNT - 2; ++i) {
                memcpy(in_buffer, out_buffer, BLAKE2B_OUTBYTES);
                Blake2BHash64_(out_buffer, in_buffer);
                memcpy(expanded + (i * HALF_OUT_BYTES), out_buffer, HALF_OUT_BYTES);
            }

            Blake2BHash64_(in_buffer, out_buffer);
            memcpy(expanded + HALF_OUT_BYTES * (HASH_BLOCKS_COUNT - 2), in_buffer, BLAKE2B_OUTBYTES);
        }

        static void Blake2BLong_(ui8* out, ui32 outlen, const ui8* in, ui32 inlen) {
            if (outlen < BLAKE2B_OUTBYTES) {
                TBlake2B<instructionSet> hash(outlen);
                hash.Update(outlen);
                hash.Update(in, inlen);
                hash.Final(out, outlen);
            } else {
                ui8 out_buffer[BLAKE2B_OUTBYTES];
                ui8 in_buffer[BLAKE2B_OUTBYTES];
                ui32 toproduce = outlen - BLAKE2B_OUTBYTES / 2;

                TBlake2B<instructionSet> hash1(BLAKE2B_OUTBYTES);
                hash1.Update(outlen);
                hash1.Update(in, inlen);
                hash1.Final(out_buffer, BLAKE2B_OUTBYTES);

                memcpy(out, out_buffer, BLAKE2B_OUTBYTES / 2);
                out += BLAKE2B_OUTBYTES / 2;

                while (toproduce > BLAKE2B_OUTBYTES) {
                    memcpy(in_buffer, out_buffer, BLAKE2B_OUTBYTES);
                    TBlake2B<instructionSet> hash2(BLAKE2B_OUTBYTES);
                    hash2.Update(in_buffer, BLAKE2B_OUTBYTES);
                    hash2.Final(out_buffer, BLAKE2B_OUTBYTES);
                    memcpy(out, out_buffer, BLAKE2B_OUTBYTES / 2);
                    out += BLAKE2B_OUTBYTES / 2;
                    toproduce -= BLAKE2B_OUTBYTES / 2;
                }

                memcpy(in_buffer, out_buffer, BLAKE2B_OUTBYTES);
                {
                    TBlake2B<instructionSet> hash3(toproduce);
                    hash3.Update(in_buffer, BLAKE2B_OUTBYTES);
                    hash3.Final(out_buffer, toproduce);
                    memcpy(out, out_buffer, toproduce);
                }
            }
        }

        static void InitBlockValue_(TBlock* b, ui8 in) {
            memset(b->V, in, sizeof(b->V));
        }

    protected: /* Functions */
        void InternalHash_(TBlock* memory, const ui8* pwd, ui32 pwdlen,
                           const ui8* salt, ui32 saltlen, ui8* out, ui32 outlen,
                           const ui8* aad, ui32 aadlen) const {
            /*
             * all parameters checks are in proxy objects
             */

            Initialize_(memory, outlen, pwd, pwdlen, salt, saltlen, aad, aadlen);
            FillMemoryBlocks_(memory);
            Finalize_(memory, out, outlen);
        }

        void InitialHash_(ui8 blockhash[ARGON2_PREHASH_DIGEST_LENGTH],
                          ui32 outlen, const ui8* pwd, ui32 pwdlen,
                          const ui8* salt, ui32 saltlen, const ui8* aad, ui32 aadlen) const {
            TBlake2B<instructionSet> hash(ARGON2_PREHASH_DIGEST_LENGTH);
            /* lanes, but lanes == threads */
            hash.Update(Lanes_);
            /* outlen */
            hash.Update(outlen);
            /* m_cost */
            hash.Update(mcost);
            /* t_cost */
            hash.Update(Tcost_);
            /* version */
            hash.Update(0x00000013);
            /* Argon2 type */
            hash.Update((ui32)Atype_);
            /* pwdlen */
            hash.Update(pwdlen);
            /* pwd */
            hash.Update(pwd, pwdlen);
            /* saltlen */
            hash.Update(saltlen);
            /* salt */
            if (saltlen)
                hash.Update(salt, saltlen);
            /* secret */
            hash.Update(SecretLen_);
            if (SecretLen_)
                hash.Update((void*)Secret_, SecretLen_);
            /* aadlen */
            hash.Update(aadlen);
            if (aadlen)
                hash.Update((void*)aad, aadlen);
            hash.Final(blockhash, ARGON2_PREHASH_DIGEST_LENGTH);
        }

        void FillFirstBlocks_(TBlock* blocks, ui8* blockhash) const {
            for (ui32 l = 0; l < Lanes_; l++) {
                /* fill the first block of the lane */
                Store32_(l, blockhash + ARGON2_PREHASH_DIGEST_LENGTH + 4);
                Store32_(0, blockhash + ARGON2_PREHASH_DIGEST_LENGTH);
                ExpandBlockhash_((ui8*)&(blocks[l * LaneLength_]), blockhash);

                /* fill the second block of the lane */
                Store32_(1, blockhash + ARGON2_PREHASH_DIGEST_LENGTH);
                ExpandBlockhash_((ui8*)&(blocks[l * LaneLength_ + 1]), blockhash);
            }
        }

        /* The 'if' will be optimized out as the number of threads is known at the compile time */
        void FillMemoryBlocks_(TBlock* memory) const {
            for (ui32 t = 0; t < Tcost_; ++t) {
                for (ui32 s = 0; s < ARGON2_SYNC_POINTS; ++s) {
                    if (Lanes_ == 1)
                        FillSegment_(memory, t, 0, s);
                    else {
                        NYmp::SetThreadCount(Lanes_);
                        NYmp::ParallelForStaticAutoChunk<ui32>(0, Lanes_, [this, &memory, s, t](int k) {
                            this->FillSegment_(memory, t, k, s);
                        });
                    }
                }
            }
        }

        void Initialize_(TBlock* memory, ui32 outlen, const ui8* pwd, ui32 pwdlen,
                         const ui8* salt, ui32 saltlen, const ui8* aad, ui32 aadlen) const {
            ui8 blockhash[ARGON2_PREHASH_SEED_LENGTH];
            InitialHash_(blockhash, outlen, pwd, pwdlen, salt, saltlen, aad, aadlen);
            FillFirstBlocks_(memory, blockhash);
        }

        ui32 ComputeReferenceArea_(ui32 pass, ui32 slice, ui32 index, bool sameLane) const {
            ui32 passVal = pass == 0 ? (slice * SegmentLength_) : (LaneLength_ - SegmentLength_);
            return sameLane ? passVal + (index - 1) : passVal + (index == 0 ? -1 : 0);
        }

        ui32 IndexAlpha_(ui32 pass, ui32 slice, ui32 index, ui32 pseudoRand, bool sameLane) const {
            ui32 referenceAreaSize = ComputeReferenceArea_(pass, slice, index, sameLane);

            ui64 relativePosition = pseudoRand;
            relativePosition = relativePosition * relativePosition >> 32;
            relativePosition = referenceAreaSize - 1 - (referenceAreaSize * relativePosition >> 32);

            ui32 startPosition = 0;
            if (pass != 0)
                startPosition = (slice == ARGON2_SYNC_POINTS - 1) ? 0 : (slice + 1) * SegmentLength_;

            return (ui32)((startPosition + relativePosition) % LaneLength_);
        }

        void NextAddresses_(TBlock* addressBlock, TBlock* inputBlock, const TBlock* zeroBlock) const {
            inputBlock->V[6]++;
            FillBlock_(zeroBlock, inputBlock, addressBlock, false);
            FillBlock_(zeroBlock, addressBlock, addressBlock, false);
        }

        void Finalize_(const TBlock* memory, ui8* out, ui32 outlen) const {
            TBlock blockhash;
            CopyBlock_(&blockhash, memory + LaneLength_ - 1);

            /* XOR the last blocks */
            for (ui32 l = 1; l < Lanes_; ++l) {
                ui32 lastBlockInLane = l * LaneLength_ + (LaneLength_ - 1);
                XorBlock_(&blockhash, memory + lastBlockInLane);
            }

            Blake2BLong_(out, outlen, (ui8*)blockhash.V, ARGON2_BLOCK_SIZE);
        }

        /* The switch will be optimized out by the compiler as the type is known at the compile time */
        void FillSegment_(TBlock* memory, ui32 pass, ui32 lane, ui32 slice) const {
            switch (Atype_) {
                case EArgon2Type::Argon2d:
                    FillSegmentD_(memory, pass, lane, slice);
                    return;
                case EArgon2Type::Argon2i:
                    FillSegmentI_(memory, pass, lane, slice, EArgon2Type::Argon2i);
                    return;
                case EArgon2Type::Argon2id:
                    if (pass == 0 && slice < ARGON2_SYNC_POINTS / 2)
                        FillSegmentI_(memory, pass, lane, slice, EArgon2Type::Argon2id);
                    else
                        FillSegmentD_(memory, pass, lane, slice);
                    return;
            }
        }

        void FillSegmentD_(TBlock* memory, ui32 pass, ui32 lane, ui32 slice) const {
            ui32 startingIndex = (pass == 0 && slice == 0) ? 2 : 0;
            ui32 currOffset = lane * LaneLength_ + slice * SegmentLength_ + startingIndex;
            ui32 prevOffset = currOffset + ((currOffset % LaneLength_ == 0) ? LaneLength_ : 0) - 1;

            for (ui32 i = startingIndex; i < SegmentLength_; ++i, ++currOffset, ++prevOffset) {
                if (currOffset % LaneLength_ == 1) {
                    prevOffset = currOffset - 1;
                }

                ui64 pseudoRand = memory[prevOffset].V[0];
                ui64 refLane = (pass == 0 && slice == 0) ? lane : (((pseudoRand >> 32)) % Lanes_);
                ui64 refIndex = IndexAlpha_(pass, slice, i, (ui32)(pseudoRand & 0xFFFFFFFF), refLane == lane);

                TBlock* refBlock = memory + LaneLength_ * refLane + refIndex;
                FillBlock_(memory + prevOffset, refBlock, memory + currOffset, pass != 0);
            }
        }

        void FillSegmentI_(TBlock* memory, ui32 pass, ui32 lane, ui32 slice, EArgon2Type atp) const {
            TBlock addressBlock, inputBlock, zeroBlock;
            InitBlockValue_(&zeroBlock, 0);
            InitBlockValue_(&inputBlock, 0);

            inputBlock.V[0] = pass;
            inputBlock.V[1] = lane;
            inputBlock.V[2] = slice;
            inputBlock.V[3] = MemoryBlocks_;
            inputBlock.V[4] = Tcost_;
            inputBlock.V[5] = (ui64)atp;

            ui32 startingIndex = 0;

            if (pass == 0 && slice == 0) {
                startingIndex = 2;
                NextAddresses_(&addressBlock, &inputBlock, &zeroBlock);
            }

            ui32 currOffset = lane * LaneLength_ + slice * SegmentLength_ + startingIndex;
            ui32 prevOffset = currOffset + ((currOffset % LaneLength_ == 0) ? LaneLength_ : 0) - 1;

            for (ui32 i = startingIndex; i < SegmentLength_; ++i, ++currOffset, ++prevOffset) {
                if (currOffset % LaneLength_ == 1) {
                    prevOffset = currOffset - 1;
                }

                if (i % ARGON2_ADDRESSES_IN_BLOCK == 0) {
                    NextAddresses_(&addressBlock, &inputBlock, &zeroBlock);
                }

                ui64 pseudoRand = addressBlock.V[i % ARGON2_ADDRESSES_IN_BLOCK];
                ui64 refLane = (pass == 0 && slice == 0) ? lane : (((pseudoRand >> 32)) % Lanes_);
                ui64 refIndex = IndexAlpha_(pass, slice, i, (ui32)(pseudoRand & 0xFFFFFFFF), refLane == lane);

                TBlock* refBlock = memory + LaneLength_ * refLane + refIndex;
                FillBlock_(memory + prevOffset, refBlock, memory + currOffset, pass != 0);
            }
        }
    };
}
