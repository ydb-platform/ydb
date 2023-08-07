#include "indexation.h"
#include "mark_granules.h"
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
    TBase::DoStart(self);
    self.BackgroundController.StartIndexing();
}

void TInsertColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BYTES_WRITTEN, context.BytesWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_TIME, context.Duration.MilliSeconds());
}

void TInsertColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishIndexing();
}

NKikimr::TConclusion<std::vector<TString>> TInsertColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_VERIFY(!DataToIndex.empty());
    Y_VERIFY(AppendedPortions.empty());

    auto maxSnapshot = TSnapshot::Zero();
    for (auto& inserted : DataToIndex) {
        TSnapshot insertSnap = inserted.GetSnapshot();
        Y_VERIFY(insertSnap.Valid());
        if (insertSnap > maxSnapshot) {
            maxSnapshot = insertSnap;
        }
    }
    Y_VERIFY(maxSnapshot.Valid());

    auto resultSchema = context.SchemaVersions.GetSchema(maxSnapshot);
    Y_VERIFY(resultSchema->GetIndexInfo().IsSorted());

    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> pathBatches;
    for (auto& inserted : DataToIndex) {
        TBlobRange blobRange(inserted.BlobId, 0, inserted.BlobId.BlobSize());

        auto blobSchema = context.SchemaVersions.GetSchema(inserted.GetSchemaSnapshot());
        auto& indexInfo = blobSchema->GetIndexInfo();
        Y_VERIFY(indexInfo.IsSorted());

        std::shared_ptr<arrow::RecordBatch> batch;
        if (auto it = CachedBlobs.find(inserted.BlobId); it != CachedBlobs.end()) {
            batch = it->second;
        } else if (auto* blobData = Blobs.FindPtr(blobRange)) {
            Y_VERIFY(!blobData->empty(), "Blob data not present");
            // Prepare batch
            batch = NArrow::DeserializeBatch(*blobData, indexInfo.ArrowSchema());
            if (!batch) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                    ("event", "cannot_parse")
                    ("data_snapshot", TStringBuilder() << inserted.GetSnapshot())
                    ("index_snapshot", TStringBuilder() << blobSchema->GetSnapshot());
            }
        } else {
            Y_VERIFY(blobData, "Data for range %s has not been read", blobRange.ToString().c_str());
        }
        Y_VERIFY(batch);

        batch = AddSpecials(batch, blobSchema->GetIndexInfo(), inserted);
        batch = resultSchema->NormalizeBatch(*blobSchema, batch);
        pathBatches[inserted.PathId].push_back(batch);
        Y_VERIFY_DEBUG(NArrow::IsSorted(pathBatches[inserted.PathId].back(), resultSchema->GetIndexInfo().GetReplaceKey()));
    }
    std::vector<TString> blobs;

    for (auto& [pathId, batches] : pathBatches) {
        AddPathIfNotExists(pathId);

        // We could merge data here cause tablet limits indexing data portions
        auto merged = NArrow::CombineSortedBatches(batches, resultSchema->GetIndexInfo().SortReplaceDescription());
        Y_VERIFY(merged);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, resultSchema->GetIndexInfo().GetReplaceKey()));

        auto granuleBatches = TMarksGranules::SliceIntoGranules(merged, PathToGranule[pathId], resultSchema->GetIndexInfo());
        for (auto& [granule, batch] : granuleBatches) {
            auto portions = MakeAppendedPortions(pathId, batch, granule, maxSnapshot, blobs, nullptr, context);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    Y_VERIFY(PathToGranule.size() == pathBatches.size());
    return blobs;
}

std::shared_ptr<arrow::RecordBatch> TInsertColumnEngineChanges::AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
    const TIndexInfo& indexInfo, const TInsertedData& inserted) const
{
    auto batch = TIndexInfo::AddSpecialColumns(srcBatch, inserted.GetSnapshot());
    Y_VERIFY(batch);

    return NArrow::ExtractColumns(batch, indexInfo.ArrowSchemaWithSpecials());
}

NColumnShard::ECumulativeCounters TInsertColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL;
}

}
