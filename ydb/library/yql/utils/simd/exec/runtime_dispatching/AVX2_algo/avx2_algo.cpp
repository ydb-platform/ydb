#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>
#include <immintrin.h>
#include <avxintrin.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<AVX2Trait>() {
    Cerr << "AVXTrait ";
    return MakeHolder<Algo<AVX2Trait>>();
}