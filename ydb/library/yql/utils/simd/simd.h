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

inline void FallbackMergeColumns(i8 *result, i8 *const data[4], size_t sizes[4],
                                 size_t length, size_t from) {
    if (length < from) {
        return;
    }

    const i8 *srcs[4];
    i8 *dst;
    size_t col_sizes[4];

    [&]<size_t... Is>(std::index_sequence<Is...>) {
      const size_t pack_size = (... + sizes[Is]);
      (..., (srcs[Is] = data[Is] + from * sizes[Is]));
      dst = result + from * pack_size;
      (..., (col_sizes[Is] = sizes[Is]));
    }(std::make_index_sequence<4>{});

    // merge_columns/Fallback_algo/fallback_algo.cpp
    void FallbackMergeBlockImpl(const i8 *const(&src_cols)[4],
                                i8 *const dst_rows, size_t size,
                                const size_t(&col_sizes)[4]);

    FallbackMergeBlockImpl(srcs, dst, length - from, col_sizes);
}

struct Perfomancer {

    Perfomancer() = default;

    struct Interface {

        virtual ~Interface() = default;

        inline virtual void MergeColumns(i8* result, i8* const data[4], size_t sizes[4], size_t length) = 0;

    };

    template <typename Trait>
    class Algo : public Interface {
    public:
        Algo() {}

        void MergeColumns(i8* result, i8* const data[4], size_t sizes[4], size_t length) override {
            std::vector<Trait> reg(16);
            std::vector<Trait> mask(15);

            int pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            int block = Trait::SIZE / pack * pack;

            PrepareMasks(sizes, mask);

            //const size_t stores = std::min(4ul, Trait::SIZE / sizes[3]);
            size_t i = 0;

            for (; i * sizes[0] + Trait::SIZE < length * sizes[0]; i += Trait::SIZE / pack * 2) {
                Iteration(sizes, data, result, i, pack * i, block, reg, mask);
            }
            FallbackMergeColumns(result, data, sizes, length, i);
        }

        void Transponation(i8* data, i8* storage[4], size_t sizes[4], size_t length) {
            Trait masks[20];
            GetMasks(masks, sizes);
            //size_t pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            i8* align_ptr = data;
            while((align_ptr - data) < (length - 32)) {
                align_ptr = Iteration(sizes, data, align_ptr, storage, masks);
            }
            //memcopy...
        }



        ~Algo() = default;
    
    private:
        Trait CreateBlendMask(size_t size1, size_t size2, bool shift) {
            i8 result[Trait::SIZE];

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
            return reg;
        }

        Trait CreateShuffleToBlendMask(size_t size1, size_t size2, bool shift) {
            int packs = Trait::SIZE / (size1 + size2);
            size_t cnt = 0;
            size_t order = 0;
            i8 result[Trait::SIZE];

            while (cnt < packs * (size1 + size2)) {

                if (shift) {
                    if (cnt % (size1 + size2) < size2) {
                        result[cnt++] = 0x80;
                    } else {
                        result[cnt++] = order++;
                    }
                } else {
                    if (cnt % (size1 + size2) < size1) {
                        result[cnt++] = order++;
                    } else {
                        result[cnt++] = 0x80;
                    }
                }
            }

            Trait reg(result);
            return reg;
        }

        Trait CreatePureShuffleMask(size_t size1, size_t size2) {

            i8 result[Trait::SIZE];
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
            reg[4] = reg[0].template Shuffle<false>(mask[0]);
            reg[5] = reg[1].template Shuffle<false>(mask[1]);
            reg[6] = reg[2].template Shuffle<false>(mask[2]);
            reg[7] = reg[3].template Shuffle<false>(mask[3]);

            //pure shuffle
            reg[8] = reg[0].template Shuffle<false>(mask[9]);
            reg[9] = reg[1].template Shuffle<false>(mask[10]);
            reg[10] = reg[2].template Shuffle<false>(mask[11]);
            reg[11] = reg[3].template Shuffle<false>(mask[12]);

            //blend
            //0101
            reg[0] = reg[4].Blend(reg[5], mask[6]);
            //2323
            reg[1] = reg[6].Blend(reg[7], mask[7]);
            
            //shuffle to blend
            reg[12] = reg[8].template Shuffle<false>(mask[0]);
            reg[13] = reg[9].template Shuffle<false>(mask[1]);
            reg[14] = reg[10].template Shuffle<false>(mask[2]); 
            reg[15] = reg[11].template Shuffle<false>(mask[3]);

            //blend
            //0101
            reg[2] = reg[12].Blend(reg[13], mask[6]);
            //2323
            reg[3] = reg[14].Blend(reg[15], mask[7]);

            reg[4] = reg[0].template Shuffle<false>(mask[4]);
            reg[5] = reg[1].template Shuffle<false>(mask[5]);

            reg[6] = reg[0].template Shuffle<false>(mask[13]);
            reg[7] = reg[1].template Shuffle<false>(mask[14]);

            reg[0] = reg[6].template Shuffle<false>(mask[4]);
            reg[1] = reg[7].template Shuffle<false>(mask[5]);
            reg[8] = reg[4].Blend(reg[5], mask[8]); //ok
            reg[9] = reg[0].Blend(reg[1], mask[8]); //ok

            reg[4] = reg[2].template Shuffle<false>(mask[4]);
            reg[5] = reg[3].template Shuffle<false>(mask[5]);

            reg[6] = reg[2].template Shuffle<false>(mask[13]);
            reg[7] = reg[3].template Shuffle<false>(mask[14]);

            reg[2] = reg[6].template Shuffle<false>(mask[4]);
            reg[3] = reg[7].template Shuffle<false>(mask[5]);

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

        void GetMasks(Trait masks[20], size_t sizes[4]) {
            MaskForCollector(masks, sizes); // 0 1 2 3 masks
            masks[4] = ShuffleBack(sizes); // 4 masks
            SimpleBlendMask(sizes, masks); // 5 6 7 8 masks
            MaskForRightOrder(masks, sizes); // 9 10 11 12
        }

        void MaskForCollector(Trait masks[20], size_t sizes[4]) {
            size_t prefsums[4]{sizes[0], 0, 0, 0};
            for (int i = 1; i < 4; ++i) {
            prefsums[i] = prefsums[i - 1] + sizes[i];
            }
            i8 res[4][Trait::SIZE];
            for (int k = 0; k < 4; ++k) {
                for (int i = 0; i < Trait::SIZE; ++i) {
                    res[k][i] = 0x80;
                }
            }
            for (int k = 0; k < 4; ++k) {
                int cnt = 0;
                for (int i = 0; i < Trait::SIZE; ++i) {
                    res[k][(cnt++) % Trait::SIZE] = ((i - sizes[k] + Trait::SIZE) % Trait::SIZE);
                }
            }
            for (int i = 0; i < 4; ++i) {
                masks[i].Get(res[i]);
            }
        }


        Trait ShuffleBack(size_t sizes[4]) {
            i8 res[Trait::SIZE];
            size_t pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            size_t num = Trait::SIZE / pack;
            size_t prefsums[4]{0, sizes[0], sizes[0] + sizes[1], sizes[0] + sizes[1] + sizes[2]};
            int cnt = 0;
            for (size_t k = 0; k < 4; ++k) {
                for (size_t i = 0; i < num; ++i) {
                    for (size_t j = 0; j < sizes[k]; ++j) {
                        res[cnt++] = prefsums[k] + j + i * pack;
                    }
                }
            }
            Trait reg;
            reg.SetMask(res);
            return reg;
        }

        void MaskForRightOrder(Trait masks[20], size_t sizes[4]) {
            size_t pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            size_t number_of_regs = Trait::SIZE / (Trait::SIZE / pack * sizes[3]);
            size_t prefsums[4]{0, 0, 0, 0};
            
            for (int i = 1; i < 4; ++i) {
                prefsums[i] = prefsums[i - 1] + sizes[i - 1];
            }
            
            i8 res[4][Trait::SIZE];
            
            for (int k = 0; k < 4; ++k) {
                for (int i = 0; i < Trait::SIZE; ++i) {
                    res[k][i] = 0x80;
                }
            }
            
            for (int k = 0; k < 4; ++k) {
                int cnt = 0;
                int start = (prefsums[k] + number_of_regs * sizes[k]) % Trait::SIZE;
                for (int i = 0; i < Trait::SIZE; ++i) {
                    res[k][(cnt++) % Trait::SIZE] = (start-- + Trait::SIZE) % Trait::SIZE;
                }
            }
            
            for (int i = 9; i < 13; ++i) {
                masks[i].Get(res[i - 9]);
            }
        }
        void SimpleBlendMask(size_t sizes[4], Trait masks[20]) {
            size_t prefsums[5]{0, 0, 0, 0, 0};
            
            for (int i = 1; i < 5; ++i) {
                prefsums[i] = prefsums[i - 1] + sizes[i - 1];
            }
            
            i8 res[4][Trait::SIZE];
            
            for (size_t i = 5; i < 9; ++i) { // cnt of columns == cnt of blend masks
                for (size_t j = 0; j < Trait::SIZE; ++j) {
                    res[i - 5][j] = 0xFF;
                }
                for (size_t j = prefsums[i - 5]; j < prefsums[i - 4]; ++j) {
                    res[i - 5][j] = 0x00;
                }
            }

            for (int i = 5; i < 9; ++i) {
                masks[i].SetMask(res[i - 5]);
            }
        }
        i8* RevertIteration(size_t sizes[4], i8* data, i8* align_ptr, i8* storage[4], Trait masks[20]) {
            size_t pack = (sizes[0] + sizes[1] + sizes[2] + sizes[3]);
            size_t cnt = Trait::SIZE / pack;
            size_t number_of_regs = Trait::SIZE / (Trait::SIZE / pack * sizes[3]);

            Trait collectors[4];
            Trait reg[number_of_regs * 2];
            
            for (size_t i = 0; i < number_of_regs; ++i) {
                reg[i].Get(align_ptr + i * (cnt * pack));
            }

            for (size_t i = 0; i < number_of_regs; ++i) {
                reg[i + number_of_regs] = reg[i].template Shuffle<false>(masks[4]);
            }

            for (size_t k = 0; k < 4; ++k) {
                for (size_t i = number_of_regs; i < number_of_regs * 2; ++i) {
                    Trait copy = reg[i].Blend(collectors[k], masks[5 + k]);
                    collectors[k] = copy.template Shuffle<false>(masks[k]);
                }
            }

            size_t shift = (align_ptr - data) / pack;

            for (size_t k = 0; k < 4; ++k) {
                Trait copy = collectors[k].template Shuffle<false>(masks[9 + k]);
                copy.Store(storage[k] + shift * sizes[k]);
            }

            return align_ptr + number_of_regs * (Trait::SIZE / pack * pack);
        }
        void MemoryCopy(size_t sizes[4], i8* data, long long length, i8* storage[4]) {
            i8* ptr = data;
            size_t pack = sizes[0] + sizes[1] + sizes[2] + sizes[3];
            i8* pointers[4]{storage[0], storage[1], storage[2], storage[3]};

            while ((ptr - data + pack) <= static_cast<size_t>(length)) {
                for (int k = 0; k < 4; ++k) {
                    memcpy(pointers[k], ptr, sizes[k]);
                    ptr += sizes[k];
                }
            }
        }
    };

    template <typename Trait>
    inline THolder<Interface> Create() {
        return MakeHolder<Interface>();
    }

};

template <> class Perfomancer::Algo<FallbackTrait> : public Interface {
    void MergeColumns(i8 *result, i8 *const data[4], size_t sizes[4],
                      size_t length) override {
        FallbackMergeColumns(result, data, sizes, length, 0);
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