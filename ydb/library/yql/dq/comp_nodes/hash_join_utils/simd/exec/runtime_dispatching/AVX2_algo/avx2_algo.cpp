#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/algo.h>
#include <immintrin.h>
#include <avxintrin.h>

template<>
std::unique_ptr<Perfomancer::Interface> Perfomancer::Create<AVX2Trait>() {
    Cerr << "AVXTrait ";
    return std::make_unique<Algo<AVX2Trait>>();
}