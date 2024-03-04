#include "indexation.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap {

bool TInsertColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    if (!TBase::DoApplyChanges(self, context)) {
        return false;
    }
    return true;
}

void TInsertColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    TBase::DoWriteIndex(self, context);
    for (const auto& insertedData : DataToIndex) {
        self.InsertTable->EraseCommitted(context.DBWrapper, insertedData);
        Y_ABORT_UNLESS(insertedData.GetBlobRange().IsFullBlob());
    }
    if (!DataToIndex.empty()) {
        self.UpdateInsertTableCounters();
    }
}

void TInsertColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    Y_ABORT_UNLESS(DataToIndex.size());
    auto removing = BlobsAction.GetRemoving(IStoragesManager::DefaultStorageId);
    auto reading = BlobsAction.GetReading(IStoragesManager::DefaultStorageId);
    for (size_t i = 0; i < DataToIndex.size(); ++i) {
        const auto& insertedData = DataToIndex[i];
        Y_ABORT_UNLESS(insertedData.GetBlobRange().IsFullBlob());
        reading->AddRange(insertedData.GetBlobRange(), insertedData.GetBlobData().value_or(""));
        removing->DeclareRemove(insertedData.GetBlobRange().GetBlobId());
    }

    self.BackgroundController.StartIndexing(*this);
}

void TInsertColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BLOBS_WRITTEN, context.BlobsWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_BYTES_WRITTEN, context.BytesWritten);
    self.IncCounter(NColumnShard::COUNTER_INDEXING_TIME, context.Duration.MilliSeconds());
}

void TInsertColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishIndexing(*this);
}

TConclusionStatus TInsertColumnEngineChanges::DoConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(!DataToIndex.empty());
    Y_ABORT_UNLESS(AppendedPortions.empty());

    auto maxSnapshot = TSnapshot::Zero();
    for (auto& inserted : DataToIndex) {
        TSnapshot insertSnap = inserted.GetSnapshot();
        Y_ABORT_UNLESS(insertSnap.Valid());
        if (insertSnap > maxSnapshot) {
            maxSnapshot = insertSnap;
        }
    }
    Y_ABORT_UNLESS(maxSnapshot.Valid());

    auto resultSchema = context.SchemaVersions.GetSchema(maxSnapshot);
    Y_ABORT_UNLESS(resultSchema->GetIndexInfo().IsSorted());

    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> pathBatches;
    for (auto& inserted : DataToIndex) {
        const TBlobRange& blobRange = inserted.GetBlobRange();

        auto blobSchema = context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion());
        auto& indexInfo = blobSchema->GetIndexInfo();
        Y_ABORT_UNLESS(indexInfo.IsSorted());

        std::shared_ptr<arrow::RecordBatch> batch;
        {
            auto itBlobData = Blobs.find(blobRange);
            Y_ABORT_UNLESS(itBlobData != Blobs.end(), "Data for range %s has not been read", blobRange.ToString().c_str());
            Y_ABORT_UNLESS(!itBlobData->second.empty(), "Blob data not present");
            // Prepare batch
            batch = NArrow::DeserializeBatch(itBlobData->second, indexInfo.ArrowSchema());
            Blobs.erase(itBlobData);
            AFL_VERIFY(batch)("event", "cannot_parse")
                ("data_snapshot", TStringBuilder() << inserted.GetSnapshot())
                ("index_snapshot", TStringBuilder() << blobSchema->GetSnapshot());
            ;
        }

        batch = AddSpecials(batch, indexInfo, inserted);
        batch = resultSchema->NormalizeBatch(*blobSchema, batch);
        batch = NArrow::SortBatch(batch, resultSchema->GetIndexInfo().GetReplaceKey(), true);
        pathBatches[inserted.PathId].push_back(batch);
        AFL_VERIFY(NArrow::IsSortedAndUnique(pathBatches[inserted.PathId].back(), resultSchema->GetIndexInfo().GetReplaceKey()));
    }

    Y_ABORT_UNLESS(Blobs.empty());
    const std::vector<std::string> comparableColumns = resultSchema->GetIndexInfo().GetReplaceKey()->field_names();
    for (auto& [pathId, batches] : pathBatches) {
        NIndexedReader::TMergePartialStream stream(resultSchema->GetIndexInfo().GetReplaceKey(), resultSchema->GetIndexInfo().ArrowSchemaWithSpecials(), false);
        THashMap<std::string, ui64> fieldSizes;
        ui64 rowsCount = 0;
        for (auto&& batch : batches) {
            stream.AddSource(batch, nullptr);
            for (ui32 cIdx = 0; cIdx < (ui32)batch->num_columns(); ++cIdx) {
                fieldSizes[batch->column_name(cIdx)] += NArrow::GetArrayDataSize(batch->column(cIdx));
            }
            rowsCount += batch->num_rows();
        }

        NIndexedReader::TRecordBatchBuilder builder(resultSchema->GetIndexInfo().ArrowSchemaWithSpecials()->fields(), rowsCount, fieldSizes);
        stream.SetPossibleSameVersion(true);
        stream.DrainAll(builder);

        auto itGranule = PathToGranule.find(pathId);
        AFL_VERIFY(itGranule != PathToGranule.end());
        std::vector<std::shared_ptr<arrow::RecordBatch>> result = NIndexedReader::TSortableBatchPosition::SplitByBordersInSequentialContainer(builder.Finalize(), comparableColumns, itGranule->second);
        for (auto&& b : result) {
            if (!b) {
                continue;
            }
            if (b->num_rows() < 100) {
                SaverContext.SetExternalCompression(NArrow::TCompression(arrow::Compression::type::UNCOMPRESSED));
            } else {
                SaverContext.SetExternalCompression(NArrow::TCompression(arrow::Compression::type::LZ4_FRAME));
            }
            auto portions = MakeAppendedPortions(b, pathId, maxSnapshot, nullptr, context);
            Y_ABORT_UNLESS(portions.size());
            for (auto& portion : portions) {
                AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    Y_ABORT_UNLESS(PathToGranule.size() == pathBatches.size());
    return TConclusionStatus::Success();
}

std::shared_ptr<arrow::RecordBatch> TInsertColumnEngineChanges::AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
    const TIndexInfo& indexInfo, const TInsertedData& inserted) const {
    auto batch = TIndexInfo::AddSpecialColumns(srcBatch, inserted.GetSnapshot());
    Y_ABORT_UNLESS(batch);

    return NArrow::ExtractColumns(batch, indexInfo.ArrowSchemaWithSpecials());
}

NColumnShard::ECumulativeCounters TInsertColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL;
}

}
