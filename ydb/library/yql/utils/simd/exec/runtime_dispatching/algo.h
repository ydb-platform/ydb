#include "util/system/cpu_id.h"
#include <ydb/library/yql/utils/simd/simd_avx2.h>
#include <ydb/library/yql/utils/simd/simd_fallback.h>
#include <ydb/library/yql/utils/simd/simd_sse42.h>
#include <vector>

using vl = std::vector<ui64>;
using vvl = std::vector<std::vector<ui64>>;

using AVX2Trait = NSimd::NAVX2::TSimd8<ui64>;

using SSE42Trait = NSimd::NSSE42::TSimd8<ui64>;

using FallbackTrait = NSimd::NFallback::FallbackTrait<ui64>;

struct Perfomancer {

    Perfomancer() = default;

    struct Interface {

        virtual ~Interface() = default;

        virtual void Add(vvl& /* columns */, vl& /* result */) = 0;
        virtual void MergeColumns(ui8* /* result */, const ui8* /* data */ [4], size_t /* sizes */ [4]) = 0;
    };


    template <typename Trait>
    class Algo : public Interface {
    public:
        void Add(vvl& columns, vl& result) override {
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            std::vector<Trait> Registers(columns.size());

            for (size_t j = 0; j < result.size(); j += Trait::SIZE / sizeof(ui64)) {

                for (size_t i = 0; i < columns.size(); ++i) {
                    Registers[i] = Trait(&columns[i][j]);
                }

                for (size_t i = 1; i < columns.size(); ++i) {
                    Registers[i] += Registers[i - 1];
                }

                Registers.back().Store(&result[j]);
            }

            Cerr << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count() << "ms\n";
        }

        void MergeColumns(ui8* result, const ui8* data[4], size_t sizes[4]) override {
            std::vector<Trait> regs(16);

            Trait mask1  = CreateShuffleToBlendMask(sizes[0], sizes[1], false);
            Trait mask2  = CreateShuffleToBlendMask(sizes[1], sizes[0], true);
            Trait mask3  = CreateShuffleToBlendMask(sizes[2], sizes[3], false);
            Trait mask4  = CreateShuffleToBlendMask(sizes[3], sizes[2], true);
            Trait mask5  = CreateBlendMask(sizes[0] + sizes[1], sizes[2] + sizes[3], false);
            Trait mask6  = CreatePureShuffleMask(sizes[0], sizes[1]);
            Trait mask7  = CreatePureShuffleMask(sizes[1], sizes[0]);
            Trait mask8  = CreatePureShuffleMask(sizes[2], sizes[3]);
            Trait mask9  = CreatePureShuffleMask(sizes[3], sizes[2]);
            Trait mask10 = CreatePureShuffleMask(sizes[0] + sizes[1], sizes[2] + sizes[3]);
            Trait mask11 = CreatePureShuffleMask(sizes[2] + sizes[3], sizes[0] + sizes[1]);
            Trait mask12 = CreateBlendMask(sizes[0], sizes[1], false);
            Trait mask13 = CreateBlendMask(sizes[2], sizes[3], false);
            Trait mask14 = CreateShuffleToBlendMask(sizes[0] + sizes[1], sizes[2] + sizes[3], false);
            Trait mask15 = CreateShuffleToBlendMask(sizes[2] + sizes[3], sizes[0] + sizes[1], true);

            regs[0].Load(data[0]);
            regs[1].Load(data[1]);
            regs[2].Load(data[2]);
            regs[3].Load(data[3]);

            regs[4] = regs[0].Shuffle<false>(mask1);
            regs[5] = regs[1].Shuffle<false>(mask2);
            regs[6] = regs[2].Shuffle<false>(mask3);
            regs[7] = regs[3].Shuffle<false>(mask4);

            regs[0] = regs[0].Shuffle<false>(mask6);
            regs[0] = regs[0].Shuffle<false>(mask1);

            regs[1] = regs[1].Shuffle<false>(mask7);
            regs[1] = regs[1].Shuffle<false>(mask2);

            regs[2] = regs[2].Shuffle<false>(mask8);
            regs[2] = regs[2].Shuffle<false>(mask3);

            regs[3] = regs[3].Shuffle<false>(mask9);
            regs[3] = regs[3].Shuffle<false>(mask4);

            regs[8] = regs[4].BlendVar(regs[5], mask12);
            regs[9] = regs[0].BlendVar(regs[1], mask12);

            regs[10] = regs[6].BlendVar(regs[7], mask13);
            regs[11] = regs[2].BlendVar(regs[3], mask13);

            regs[0] = regs[8].Shuffle<false>(mask14);
            regs[2] = regs[10].Shuffle<false>(mask15);

            regs[8]  = regs[8].Shuffle<false>(mask10);
            regs[10] = regs[10].Shuffle<false>(mask11);

            regs[8]  = regs[8].Shuffle<false>(mask14);
            regs[10] = regs[10].Shuffle<false>(mask15);

            regs[12] = regs[0].BlendVar(regs[2], mask5);
            regs[13] = regs[8].BlendVar(regs[10], mask5);

            regs[1] = regs[9].Shuffle<false>(mask14);
            regs[3] = regs[11].Shuffle<false>(mask15);

            regs[9]  = regs[9].Shuffle<false>(mask10);
            regs[11] = regs[11].Shuffle<false>(mask11);

            regs[9]  = regs[9].Shuffle<false>(mask14);
            regs[11] = regs[11].Shuffle<false>(mask15);

            regs[14] = regs[1].BlendVar(regs[3], mask5);
            regs[15] = regs[9].BlendVar(regs[11], mask5);

            regs[12].Store(result);
        }

    private:
        Trait CreateBlendMask(size_t size1, size_t size2, bool shift) {
            i8 result[Trait::SIZE];
            size_t cnt = 0;

            if (shift) {
                for (size_t i = 0; i < size2; ++i) {
                    result[cnt++] = 0;
                }
            }

            while (cnt + size1 + size2 <= Trait::SIZE) {
                for (size_t i = 0; i < size1; ++i) {
                    result[cnt++] = 0x80;
                }

                if (cnt + size2 > Trait::SIZE) {
                    break;
                }

                for (size_t i = 0; i < size2; ++i) {
                    result[cnt++] = 0;
                }
            }

            Trait reg{result};
            return reg;
        }

        Trait CreateShuffleToBlendMask(size_t size1, size_t size2, bool shift) {
            i8 result[Trait::SIZE]{0};
            size_t packs = Trait::SIZE / (size1 + size2);
            size_t cnt = 0;
            size_t order = 0;

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

            Trait reg{result};
            return reg;
        }

        Trait CreatePureShuffleMask(size_t size1, size_t size2) {
            i8 result[Trait::SIZE];
            size_t packs = Trait::SIZE / (size1 + size2);
            size_t cnt = 0;
            size_t start = packs * size1;

            //start with pack * size1 
            while (cnt < packs * size1) {
                result[cnt++] = start++;
            }

            start = 0;
            while (cnt < Trait::SIZE) {
                result[cnt++] = start++;
            }

            Trait reg{result};
            return reg;
        } 
    };

    template <typename Trait>
    THolder<Interface> Create() {
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

//this part of code just to compare times of work
//we dont need this functions at all
template <typename TFactory>
auto ChooseAVX2Trait(TFactory& factory) {
    return factory.template Create<AVX2Trait>();
}

template <typename TFactory>
auto ChooseSSE42Trait(TFactory& factory) {
    return factory.template Create<SSE42Trait>();
}

template <typename TFactory>
auto ChooseFallbackTrait(TFactory& factory) {
    return factory.template Create<FallbackTrait>();
}