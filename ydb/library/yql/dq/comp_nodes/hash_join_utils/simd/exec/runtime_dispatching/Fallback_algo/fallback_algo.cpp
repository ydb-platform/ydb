#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/algo.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return MakeHolder<Algo<FallbackTrait>>();
}