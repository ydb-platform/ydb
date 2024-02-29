#include <ydb/library/yql/utils/simd/exec/merge_columns/merge.h>
#include <immintrin.h>
#include <avxintrin.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<AVX2Trait>() {
    Cerr << "AVXTrait ";
    return MakeHolder<Algo<AVX2Trait>>();
}