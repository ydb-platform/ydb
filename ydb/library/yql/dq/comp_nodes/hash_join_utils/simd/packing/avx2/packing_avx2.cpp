#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/packing_simd_impl.h>

namespace NKikimr::NMiniKQL::NPackedTuple {

template struct SIMDPack<NSimd::TSimdAVX2Traits>;

} // namespace NKikimr::NMiniKQL::NPackedTuple
