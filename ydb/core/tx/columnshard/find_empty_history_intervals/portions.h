#pragma once

#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

template <class TExcluder>
void ExcludePortionBlobs(const NOlap::TPortionDataAccessor& accessor, const NOlap::TIndexInfo& index, TExcluder&& excluder) {
    const auto& portion = accessor.GetPortionInfo();
    // External-tier data and independent indexes can use different storages.
    for (const auto& record : accessor.GetRecordsVerified()) {
        if (portion.GetColumnStorageId(record.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            excluder(accessor.GetBlobId(record.GetBlobRange().GetBlobIdxVerified()));
        }
    }
    for (const auto& indexChunk : accessor.GetIndexesVerified()) {
        if (portion.GetIndexStorageId(indexChunk.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            if (const auto range = indexChunk.GetBlobRangeOptional()) {
                excluder(accessor.GetBlobId(range->GetBlobIdxVerified()));
            }
        }
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
