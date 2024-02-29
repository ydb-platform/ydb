#include <ydb/library/yql/utils/simd/exec/merge_columns/merge.h>
#include <immintrin.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<SSE42Trait>() {
    Cerr << "SSETrait ";
    return MakeHolder<Algo<SSE42Trait>>();
}