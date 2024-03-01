#include <ydb/library/yql/utils/simd/simd.h>

template<>
THolder<NSimd::Perfomancer::Interface> NSimd::Perfomancer::Create<NSimd::FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return MakeHolder<Algo<FallbackTrait>>();
}