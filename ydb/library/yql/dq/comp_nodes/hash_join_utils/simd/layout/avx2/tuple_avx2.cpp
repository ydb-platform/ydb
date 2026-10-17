#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple_simd_impl.h>

namespace NKikimr::NMiniKQL::NPackedTuple {

THolder<TTupleLayout>
CreateAVX2TupleLayout(const std::vector<TColumnDesc>& columns) {
    return MakeHolder<TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>>(columns);
}

} // namespace NKikimr::NMiniKQL::NPackedTuple
