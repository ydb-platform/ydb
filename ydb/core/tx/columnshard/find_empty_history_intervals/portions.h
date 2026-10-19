#pragma once

#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

inline bool MayHaveBlobsInBlobStorage(const NOlap::TPortionInfo& portion) {
    const auto& bsIndexBlobBytes = portion.GetMeta().GetBsIndexBlobBytes();
    // Older portions may not have the summary: absence cannot prove there are no BS indexes.
    return portion.IsDefaultTier(NOlap::NBlobOperations::TGlobal::DefaultStorageId) || !bsIndexBlobBytes || *bsIndexBlobBytes != 0;
}

template <class TVisitor>
void VisitBlobStorageBlobs(const NOlap::TPortionDataAccessor& accessor, const NOlap::TIndexInfo& index, TVisitor&& visitor) {
    const auto& portion = accessor.GetPortionInfo();
    // External-tier data and independent indexes can use different storages.
    for (const auto& record : accessor.GetRecordsVerified()) {
        if (portion.GetColumnStorageId(record.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            visitor(accessor.GetBlobId(record.GetBlobRange().GetBlobIdxVerified()));
        }
    }
    for (const auto& indexChunk : accessor.GetIndexesVerified()) {
        if (portion.GetIndexStorageId(indexChunk.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            if (const auto range = indexChunk.GetBlobRangeOptional()) {
                visitor(accessor.GetBlobId(range->GetBlobIdxVerified()));
            }
        }
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
