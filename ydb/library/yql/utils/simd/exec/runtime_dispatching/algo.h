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

        inline virtual void Add(vvl& columns, vl& result) {

            // to avoid clang(-Wunused-parameter)
            columns[0];
            result[0];
        }

    };


    template <typename Trait>
    struct Algo : Interface {

        Algo() {}

        inline void Add(vvl& columns, vl& result) override {
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