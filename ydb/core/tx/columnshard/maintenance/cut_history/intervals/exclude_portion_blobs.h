#pragma once

#include <ydb/core/tx/columnshard/engines/portions/portion_chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

template <class TExcluder>
void ExcludePortionBlobs(
    const NOlap::TPortionInfo& portion, const NOlap::TPortionChunks& chunks, const NOlap::TIndexInfo& index, TExcluder&& excluder) {
    // External-tier data and independent indexes can use different storages.
    for (const auto& record : chunks.GetRecordsVerified()) {
        if (portion.GetColumnStorageId(record.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            excluder(chunks.GetBlobId(record.GetBlobRange().GetBlobIdxVerified()));
        }
    }
    for (const auto& indexChunk : chunks.GetIndexesVerified()) {
        if (portion.GetIndexStorageId(indexChunk.GetEntityId(), index) == NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
            if (const auto range = indexChunk.GetBlobRangeOptional()) {
                excluder(chunks.GetBlobId(range->GetBlobIdxVerified()));
            }
        }
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
