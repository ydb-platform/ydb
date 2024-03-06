#pragma once

#include <util/system/cpu_id.h>
#include <util/system/types.h>
#include <vector>
#include <stdlib.h>

#include "simd_avx2.h"
#include "simd_sse42.h"
#include "simd_fallback.h"

namespace NSimd {

template<int RegisterSize, typename TBaseRegister, template<typename> typename TSimd>
struct TSimdTraits {
    using TRegister = TBaseRegister;
    template<typename T>
    using TSimd8 = TSimd<T>;
    using TSimdI8 = TSimd8<i8>;
    static constexpr int Size = RegisterSize;
};

using TSimdAVX2Traits = TSimdTraits<32, __m256i, NSimd::NAVX2::TSimd8>;
using TSimdSSE42Traits = TSimdTraits<16, __m128i, NSimd::NSSE42::TSimd8>;
using TSimdFallbackTraits = TSimdTraits<8, ui64, NSimd::NFallback::TSimd8>;


template<typename TFactory>
auto SelectSimdTraits(const TFactory& factory) {
    if (NX86::HaveAVX2()) {
        return factory.template Create<TSimdAVX2Traits>();
    } else {
        return factory.template Create<TSimdSSE42Traits>();
    }
}

// Creates unpack mask for Simd register content. dataSize - value in bytes to unpack, stripeSize - distance between content parts.
// when needOffset is true, first data part starts at stipeSize bytes in result register
template<typename TTraits>
auto CreateUnpackMask(ui32 dataSize, ui32 stripeSize, bool needOffset) {

    using TSimdI8 = typename TTraits::template TSimd8<i8>;
    i8 indexes[TTraits::Size];

    bool insideStripe = needOffset;
    ui32 stripeOffset = 0;
    ui32 currOffset = 0;
    ui32 dataOffset = 0;
    ui32 currDataSize = 0;

    while ( currOffset < TTraits::Size) {
        if (insideStripe) {
            if (stripeOffset >= stripeSize) {
                insideStripe = false;
                currDataSize = 0;
                stripeOffset = 0;
            } else {
                indexes[currOffset++] = -1;
                stripeOffset++;
            }
        } else {
            indexes[currOffset++] = dataOffset++;
            currDataSize++;
            if (currDataSize >= dataSize) {
                insideStripe = true;
                currDataSize = 0;
                stripeOffset = 0;
            }
        }
    }

    return TSimdI8(indexes);
}


// Creates mask to advance register content for N bytes. When N is negative, move data to lower bytes.
template<typename TTraits> auto AdvanceBytesMask(const int N) {
    i8 positions[TTraits::Size];
    if (N < 0) {
        for (int i = 0; i < TTraits::Size; i += 1) {
            positions[i] = -N + i > (TTraits::Size - 1) ? -1 : -N + i;
        }
    } else {
        for (int i = 0; i < TTraits::Size; i += 1) {
            positions[i] = -N + i < 0 ? -1 : -N + i;
        }
    }
    return typename TTraits::TSimdI8(positions);
}


// Prepare unpack mask to merge two columns in one register. col1Bytes, col2Bytes - size of data in columns.
template<typename TTraits>
void PrepareMergeMasks( ui32 col1Bytes, ui32 col2Bytes, typename TTraits::TSimdI8& unpackMask1, typename TTraits::TSimdI8& unpackMask2) {
    unpackMask1 = CreateUnpackMask<TTraits>(col1Bytes, col2Bytes, false);
    unpackMask2 = CreateUnpackMask<TTraits>(col2Bytes, col1Bytes, true);
}

using AVX2Trait = NSimd::NAVX2::TSimd8<i8>;

using SSE42Trait = NSimd::NSSE42::TSimd8<i8>;

using FallbackTrait = NSimd::NFallback::FallbackTrait<i8>;

struct Perfomancer {

    Perfomancer() = default;

    struct Interface {

        virtual ~Interface() = default;

        inline virtual void MergeColumns(i8* result, i8* const data[4], size_t sizes[4], size_t length) = 0;

    };

    template <typename Trait>
    struct Algo : Interface {

        Algo() {}

        Trait CreateBlendMask(size_t size1, size_t size2, bool shift) {
            
            i8* result = new i8[Trait::SIZE];

            size_t cnt = 0;

            if (shift) {
                for (size_t i = 0; i < size2; ++i) {
                    result[cnt++] = 0xFF;
                }
            }

            while (cnt + size1 + size2 <= Trait::SIZE) {

                for (size_t i = 0; i < size1; ++i) {
                    result[cnt++] = 0x00;
                }

                if (cnt + size2 > Trait::SIZE) break;

                for (size_t i = 0; i < size2; ++i) {
                    result[cnt++] = 0xFF;
                }
            }

            Trait reg;
            reg.SetMask(result);
            delete[] result;
            return reg;
        }

        Trait CreateShuffleToBlendMask(size_t size1, size_t size2, bool shift) {
            int packs = Trait::SIZE / (size1 + size2);
            size_t cnt = 0;
            size_t order = 0;
            i8* result = new i8[Trait::SIZE];

            for (size_t i = 0; i < Trait::SIZE; ++i) {
                result[i] = 0;
            }

            if (shift) {
                while (cnt < size2) {
                    result[cnt++] = 0;
                }
            }

            while (cnt < packs * (size1 + size2)) {

                if (shift) {
                    if (cnt % (size1 + size2) < size2) {
                        result[cnt++] = 0;
                    } else {
                        result[cnt++] = order++;
                    }
                } else {
                    if (cnt % (size1 + size2) < size1) {
                        result[cnt++] = order++;
                    } else {
                        result[cnt++] = 0;
                    }
                }
            }

            Trait reg(result);
            delete[] result;
            return reg;
        }

        Trait CreatePureShuffleMask(size_t size1, size_t size2) {

            i8* result = new i8[Trait::SIZE];

            size_t packs = Trait::SIZE / (size1 + size2);
            size_t cnt = 0;
            size_t start = packs * size1;

            while (cnt < packs * size1) {
                result[cnt++] = start++;
            }

            start = 0;

            while (cnt < Trait::SIZE) {
                result[cnt++] = start++;
            }

            Trait reg(result);
            delete[] result;
            return reg;
        }
        void PrepareMasks(size_t sizes[4], std::vector<Trait>& mask) {

            mask[0] = CreateShuffleToBlendMask(sizes[0], sizes[1], false);
            mask[1] = CreateShuffleToBlendMask(sizes[1], sizes[0], true);
            mask[2] = CreateShuffleToBlendMask(sizes[2], sizes[3], false);
            mask[3] = CreateShuffleToBlendMask(sizes[3], sizes[2], true);
            mask[4] = CreateShuffleToBlendMask(sizes[0] + sizes[1], sizes[2] + sizes[3], false);
            mask[5] = CreateShuffleToBlendMask(sizes[2] + sizes[3], sizes[0] + sizes[1], true);

            mask[6] = CreateBlendMask(sizes[0], sizes[1], false);
            mask[7] = CreateBlendMask(sizes[2], sizes[3], false);
            mask[8] = CreateBlendMask(sizes[0] + sizes[1], sizes[2] + sizes[3], false);

            mask[9] = CreatePureShuffleMask(sizes[0], sizes[1]);
            mask[10] = CreatePureShuffleMask(sizes[1], sizes[0]);
            mask[11] = CreatePureShuffleMask(sizes[2], sizes[3]);
            mask[12] = CreatePureShuffleMask(sizes[3], sizes[2]);
            mask[13] = CreatePureShuffleMask(sizes[0] + sizes[1], sizes[2] + sizes[3]);
            mask[14] = CreatePureShuffleMask(sizes[2] + sizes[3], sizes[0] + sizes[1]);
        }

        void Iteration(size_t sizes[4], i8* const data[4], i8* result, int ind, int addr, int step, std::vector<Trait>& reg, std::vector<Trait>& mask) {

            reg[0].Get(&data[0][ind * sizes[0]]);
            reg[1].Get(&data[1][ind * sizes[1]]);
            reg[2].Get(&data[2][ind * sizes[2]]);
            reg[3].Get(&data[3][ind * sizes[3]]);

            //shuffle to blend
            reg[4] = reg[0].Shuffle128(mask[0]);
            reg[5]= reg[1].Shuffle128(mask[1]);
            reg[6] = reg[2].Shuffle128(mask[2]);
            reg[7] = reg[3].Shuffle128(mask[3]);

            //pure shuffle
            reg[8] = reg[0].Shuffle128(mask[9]);
            reg[9] = reg[1].Shuffle128(mask[10]);
            reg[10] = reg[2].Shuffle128(mask[11]);
            reg[11] = reg[3].Shuffle128(mask[12]);

            //blend
            //0101
            reg[0] = reg[4].Blend(reg[5], mask[6]);
            //2323
            reg[1] = reg[6].Blend(reg[7], mask[7]);

            //shuffle to blend
            reg[12] = reg[8].Shuffle128(mask[0]);
            reg[13] = reg[9].Shuffle128(mask[1]);
            reg[14] = reg[10].Shuffle128(mask[2]); 
            reg[15] = reg[11].Shuffle128(mask[3]);

            //blend
            //0101
            reg[2] = reg[12].Blend(reg[13], mask[6]);
            //2323
            reg[3] = reg[14].Blend(reg[15], mask[7]);

            reg[4] = reg[0].Shuffle128(mask[4]);
            reg[5] = reg[1].Shuffle128(mask[5]);

            reg[6] = reg[0].Shuffle128(mask[13]);
            reg[7] = reg[1].Shuffle128(mask[14]);

            reg[0] = reg[6].Shuffle128(mask[4]);
            reg[1] = reg[7].Shuffle128(mask[5]);
            reg[8] = reg[4].Blend(reg[5], mask[8]); //ok
            reg[9] = reg[0].Blend(reg[1], mask[8]); //ok

            reg[4] = reg[2].Shuffle128(mask[4]);
            reg[5] = reg[3].Shuffle128(mask[5]);

            reg[6] = reg[2].Shuffle128(mask[13]);
            reg[7] = reg[3].Shuffle128(mask[14]);

            reg[2] = reg[6].Shuffle128(mask[4]);
            reg[3] = reg[7].Shuffle128(mask[5]);

            reg[11] = reg[4].Blend(reg[5], mask[8]); //ok
            reg[12] = reg[2].Blend(reg[3], mask[8]); //ok

            reg[8].Store(&result[addr]);
            reg[9].Store(&result[addr + step]);
            reg[11].Store(&result[addr + 2 * step]);
            reg[12].Store(&result[addr + 3 * step]);
        }

        void MergeEnds(i8* result, i8* const data[4], size_t sizes[4], size_t length, size_t ind, int addr) {

            while (ind < length) {
                for (int i = 0; i < 4; ++i) {
                    memcpy(&result[addr], &data[i][ind * sizes[i]], sizes[i]);
                    addr += sizes[i];
                }
                ind++;
            }
        }

        void MergeColumns(i8* result, i8* const data[4], size_t sizes[4], size_t length) override {

            std::vector<Trait> reg(16);
            std::vector<Trait> mask(15);

            int pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            int block = Trait::SIZE / pack * pack;

            PrepareMasks(sizes, mask);

            size_t i = 0;

            for (; i * sizes[0] + Trait::SIZE < length * sizes[0]; i += Trait::SIZE / pack) {
                Iteration(sizes, data, result, i, block * i, block, reg, mask);
            }

            MergeEnds(result, data, sizes, length, i, block * i);

        }

        ~Algo() = default;
    };

    template <typename Trait>
    inline THolder<Interface> Create() {
        return MakeHolder<Interface>();
    }

};

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<AVX2Trait>();

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<SSE42Trait>();

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<FallbackTrait>();

template <typename TFactory>
auto ChooseTrait(TFactory& factory) {
    
    if (NX86::HaveAVX2()) {
        return factory.template Create<AVX2Trait>();
    
    } else if (NX86::HaveSSE42()) {
        return factory.template Create<SSE42Trait>();
    
    }
    
    return factory.template Create<FallbackTrait>();
}


}