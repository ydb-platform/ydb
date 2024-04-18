#include <ydb/library/yql/utils/simd/simd.h>
#include <immintrin.h>
#include <avxintrin.h>

template<>
THolder<NSimd::Perfomancer::Interface> NSimd::Perfomancer::Create<NSimd::AVX2Trait>() {
    Cerr << "AVXTrait ";
    return MakeHolder<Algo<AVX2Trait>>();
}