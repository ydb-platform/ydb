#include "mkql_block_map_join_utils.h"
#include "mkql_rh_hash_utils.h"

namespace NKikimr::NMiniKQL {

ui64 EstimateBlockMapJoinIndexSize(ui64 rowsCount) {
    return CalculateRHHashTableCapacity(rowsCount) * BlockMapJoinIndexEntrySize;
}

} // namespace NKikimr::NMiniKQL
