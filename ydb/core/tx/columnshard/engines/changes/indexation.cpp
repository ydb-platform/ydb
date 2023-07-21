#include "indexation.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>

namespace NKikimr::NOlap {

THashMap<NKikimr::NOlap::TUnifiedBlobId, std::vector<NKikimr::NOlap::TBlobRange>> TInsertColumnEngineChanges::GetGroupedBlobRanges() const {
    THashMap<TUnifiedBlobId, std::vector<TBlobRange>> result;
    for (size_t i = 0; i < DataToIndex.size(); ++i) {
        auto& blobId = DataToIndex[i].BlobId;
        if (CachedBlobs.contains(blobId)) {
            continue;
        }
        Y_VERIFY(!result.contains(blobId));
        std::vector<TBlobRange> blobsVector = { NBlobCache::TBlobRange(blobId, 0, blobId.BlobSize()) };
        result.emplace(blobId, std::move(blobsVector));
    }
    return result;
}

bool TInsertColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
    if (!TBase::DoApplyChanges(self, context, dryRun)) {
        return false;
    }
    return true;
}

void TInsertColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    TBase::DoWriteIndex(self, context);
    for (const auto& cmtd : DataToIndex) {
        self.InsertTable->EraseCommitted(context.DBWrapper, cmtd);
        self.BlobManager->DeleteBlob(cmtd.BlobId, *context.BlobManagerDb);
        self.BatchCache.EraseCommitted(cmtd.BlobId);
    }
    if (!DataToIndex.empty()) {
        self.UpdateInsertTableCounters();
    }
}

bool TInsertColumnEngineChanges::AddPathIfNotExists(ui64 pathId) {
    if (PathToGranule.contains(pathId)) {
        return false;
    }

    Y_VERIFY(FirstGranuleId && ReservedGranuleIds);
    ui64 granule = FirstGranuleId;
    ++FirstGranuleId;
    --ReservedGranuleIds;

    NewGranules.emplace(granule, std::make_pair(pathId, DefaultMark));
    PathToGranule[pathId].emplace_back(DefaultMark, granule);
    return true;
}

void TInsertColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartIndexing();
}

void TInsertColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    self.IncCounter(context.FinishedSuccessfully ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BYTES_WRITTEN, context.BytesWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_TIME, context.Duration.MilliSeconds());
}

void TInsertColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishIndexing();
}

}
