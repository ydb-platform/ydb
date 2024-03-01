#include <ydb/library/yql/utils/simd/simd.h>
#include <immintrin.h>

template<>
THolder<NSimd::Perfomancer::Interface> NSimd::Perfomancer::Create<NSimd::SSE42Trait>() {
    Cerr << "SSETrait ";
    return MakeHolder<Algo<SSE42Trait>>();
}