#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/algo.h>

template<>
std::unique_ptr<Perfomancer::Interface> Perfomancer::Create<FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return std::make_unique<Algo<FallbackTrait>>();
}