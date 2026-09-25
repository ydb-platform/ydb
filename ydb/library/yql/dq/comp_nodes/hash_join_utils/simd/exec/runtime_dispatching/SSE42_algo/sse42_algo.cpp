#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/algo.h>
#include <immintrin.h>

template<>
std::unique_ptr<Perfomancer::Interface> Perfomancer::Create<SSE42Trait>() {
    Cerr << "SSETrait ";
    return std::make_unique<Algo<SSE42Trait>>();
}