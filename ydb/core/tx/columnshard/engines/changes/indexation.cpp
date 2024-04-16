#include "indexation.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/formats/arrow/serializer/native.h>

namespace NKikimr::NOlap {

void TInsertColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    TBase::DoWriteIndexOnExecute(self, context);
    auto removing = BlobsAction.GetRemoving(IStoragesManager::DefaultStorageId);
    for (const auto& insertedData : DataToIndex) {
        self.InsertTable->EraseCommittedOnExecute(context.DBWrapper, insertedData, removing);
    }
}

void TInsertColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    Y_ABORT_UNLESS(DataToIndex.size());
    auto reading = BlobsAction.GetReading(IStoragesManager::DefaultStorageId);
    for (auto&& insertedData : DataToIndex) {
        reading->AddRange(insertedData.GetBlobRange(), insertedData.GetBlobData().value_or(""));
    }

    self.BackgroundController.StartIndexing(*this);
}

void TInsertColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    for (const auto& insertedData : DataToIndex) {
        self.InsertTable->EraseCommittedOnComplete(insertedData);
    }
    if (!DataToIndex.empty()) {
        self.UpdateInsertTableCounters();
    }
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
        pathBatches[inserted.PathId].push_back(batch);
        Y_DEBUG_ABORT_UNLESS(NArrow::IsSorted(pathBatches[inserted.PathId].back(), resultSchema->GetIndexInfo().GetReplaceKey()));
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
                SaverContext.SetExternalSerializer(NArrow::NSerialization::TSerializerContainer(std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::UNCOMPRESSED)));
            } else {
                SaverContext.SetExternalSerializer(NArrow::NSerialization::TSerializerContainer(std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::LZ4_FRAME)));
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
