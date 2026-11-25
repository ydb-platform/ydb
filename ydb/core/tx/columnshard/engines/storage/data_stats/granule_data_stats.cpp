#include "granule_data_stats.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

NOlap::TSimplePortionsGroupInfo& TGranuleDataStats::GetGroup(const NOlap::TPortionInfo& portion) {
    const bool evicted = !portion.IsDefaultTier(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
    if (portion.HasRemoveSnapshot()) {
        return evicted ? RemovedEvicted : Removed;
    }
    const bool written = portion.GetPortionType() == NOlap::EPortionType::Written;
    if (evicted) {
        return written ? EvictedWritten : EvictedCompacted;
    }
    return written ? Written : Compacted;
}

void TGranuleDataStats::AddPortion(const NOlap::TPortionInfo& portion, const ui64 smallBlobThresholdBytes) {
    GetGroup(portion).AddPortion(portion);
    const ui64 smallBlobsBytes = portion.GetSmallBlobBytesInBlobStorage(smallBlobThresholdBytes);
    SmallBlobsBytes += (i64)smallBlobsBytes;
    SmallBlobsCount += smallBlobsBytes ? 1 : 0;
}

void TGranuleDataStats::RemovePortion(const NOlap::TPortionInfo& portion, const ui64 smallBlobThresholdBytes) {
    GetGroup(portion).RemovePortion(portion);
    const ui64 smallBlobsBytes = portion.GetSmallBlobBytesInBlobStorage(smallBlobThresholdBytes);
    SmallBlobsBytes -= (i64)smallBlobsBytes;
    SmallBlobsCount -= smallBlobsBytes ? 1 : 0;
}

bool TGranuleDataStats::IsEmpty() const {
    return Written.IsEmpty() && Compacted.IsEmpty() && EvictedWritten.IsEmpty() && EvictedCompacted.IsEmpty() && Removed.IsEmpty() &&
           RemovedEvicted.IsEmpty();
}

}   // namespace NKikimr::NColumnShard
