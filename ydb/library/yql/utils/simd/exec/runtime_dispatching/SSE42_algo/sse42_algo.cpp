#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>
#include <immintrin.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<SSE42Trait>() {
    Cerr << "SSETrait ";
    return MakeHolder<Algo<SSE42Trait>>();
}