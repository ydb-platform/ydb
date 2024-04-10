#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return MakeHolder<Algo<FallbackTrait>>();
}