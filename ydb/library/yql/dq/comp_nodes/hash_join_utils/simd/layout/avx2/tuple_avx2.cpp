// Shared headers go before the pragma: their inline functions are also called on CPUs without AVX2.
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/hashes_calc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple_bitmatrix.h>

#include <algorithm>
#include <array>
#include <queue>

#pragma clang attribute push(__attribute__((target("avx2"))), apply_to = function)
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple_simd_impl.h>
#pragma clang attribute pop

namespace NKikimr::NMiniKQL::NPackedTuple {

THolder<TTupleLayout>
CreateAVX2TupleLayout(const std::vector<TColumnDesc>& columns) {
    return MakeHolder<TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>>(columns);
}

} // namespace NKikimr::NMiniKQL::NPackedTuple
