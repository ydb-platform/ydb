#include "indexation.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

namespace NKikimr::NOlap {

void TInsertColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    TBase::DoWriteIndexOnExecute(self, context);
    if (self) {
        auto removing = BlobsAction.GetRemoving(IStoragesManager::DefaultStorageId);
        for (const auto& insertedData : DataToIndex) {
            self->InsertTable->EraseCommittedOnExecute(context.DBWrapper, insertedData, removing);
        }
    }
}

void TInsertColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    TBase::DoStart(self);
    Y_ABORT_UNLESS(DataToIndex.size());
    auto reading = BlobsAction.GetReading(IStoragesManager::DefaultStorageId);
    for (auto&& insertedData : DataToIndex) {
        reading->AddRange(insertedData.GetBlobRange(), insertedData.GetBlobData());
    }

    self.BackgroundController.StartIndexing(*this);
}

void TInsertColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    TBase::DoWriteIndexOnComplete(self, context);
    if (self) {
        for (const auto& insertedData : DataToIndex) {
            self->InsertTable->EraseCommittedOnComplete(insertedData);
        }
        if (!DataToIndex.empty()) {
            self->UpdateInsertTableCounters();
        }
        self->IncCounter(NColumnShard::COUNTER_INDEXING_BLOBS_WRITTEN, context.BlobsWritten);
        self->IncCounter(NColumnShard::COUNTER_INDEXING_BYTES_WRITTEN, context.BytesWritten);
        self->IncCounter(NColumnShard::COUNTER_INDEXING_TIME, context.Duration.MilliSeconds());
    }
}

void TInsertColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishIndexing(*this);
}

namespace {

class TBatchInfo {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Batch);
    const NEvWrite::EModificationType ModificationType;
public:
    TBatchInfo(const std::shared_ptr<NArrow::TGeneralContainer>& batch, const NEvWrite::EModificationType modificationType)
        : Batch(batch)
        , ModificationType(modificationType)
    {

    }

    bool GetIsDeletion() const {
        return ModificationType == NEvWrite::EModificationType::Delete;
    }
};

class TPathData {
private:
    std::vector<TBatchInfo> Batches;
    YDB_READONLY_DEF(std::optional<TGranuleShardingInfo>, ShardingInfo);
    bool HasDeletionFlag = false;
public:
    TPathData(const std::optional<TGranuleShardingInfo>& shardingInfo)
        : ShardingInfo(shardingInfo)
    {
    
    }

    bool HasDeletion() {
        return HasDeletionFlag;
    }

    void AddBatch(const NOlap::TInsertedData& data, const std::shared_ptr<NArrow::TGeneralContainer>& batch) {
        if (data.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete) {
            HasDeletionFlag = true;
        }
        AFL_VERIFY(batch);
        Batches.emplace_back(batch, data.GetMeta().GetModificationType());
    }

    void AddShardingInfo(const std::optional<TGranuleShardingInfo>& info) {
        if (!info) {
            ShardingInfo.reset();
        } else if (ShardingInfo && info->GetSnapshotVersion() < ShardingInfo->GetSnapshotVersion()) {
            ShardingInfo = info;
        }
    }

    std::shared_ptr<arrow::RecordBatch> Merge(const TIndexInfo& indexInfo) const {
        auto fullSchema = indexInfo.ArrowSchemaWithSpecials();
        NArrow::NMerger::TMergePartialStream stream(indexInfo.GetReplaceKey(), fullSchema, false, IIndexInfo::GetSnapshotColumnNames());
        THashMap<std::string, ui64> fieldSizes;
        ui64 rowsCount = 0;
        for (auto&& batch : Batches) {
            auto& forMerge = batch.GetBatch();
            stream.AddSource(forMerge, nullptr);
            for (ui32 cIdx = 0; cIdx < (ui32)forMerge->GetColumnsCount(); ++cIdx) {
                fieldSizes[forMerge->GetSchema()->GetFieldVerified(cIdx)->name()] += forMerge->GetColumnVerified(cIdx)->GetRawSize().value_or(0);
            }
            rowsCount += forMerge->num_rows();
        }

        NArrow::NMerger::TRecordBatchBuilder builder(fullSchema->fields(), rowsCount, fieldSizes);
        stream.SetPossibleSameVersion(true);
        stream.DrainAll(builder);
        return builder.Finalize();
    }
};

class TPathesData {
private:
    THashMap<ui64, TPathData> Data;

public:
    const THashMap<ui64, TPathData>& GetData() const {
        return Data;
    }

    void Add(const NOlap::TInsertedData& inserted, const std::optional<TGranuleShardingInfo>& info,
        const std::shared_ptr<NArrow::TGeneralContainer>& batch) {
        auto it = Data.find(inserted.PathId);
        if (it == Data.end()) {
            it = Data.emplace(inserted.PathId, info).first;
        }
        it->second.AddShardingInfo(info);
        it->second.AddBatch(inserted, batch);
    }
};
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

    TPathesData pathBatches;
    for (auto& inserted : DataToIndex) {
        const TBlobRange& blobRange = inserted.GetBlobRange();

        auto shardingFilterCommit = context.SchemaVersions.GetShardingInfoOptional(inserted.PathId, inserted.GetSnapshot());

        auto blobSchema = context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion());
        auto& indexInfo = blobSchema->GetIndexInfo();
        Y_ABORT_UNLESS(indexInfo.IsSorted());

        std::shared_ptr<NArrow::TGeneralContainer> batch;
        {
            const auto blobData = Blobs.Extract(IStoragesManager::DefaultStorageId, blobRange);
            Y_ABORT_UNLESS(blobData.size(), "Blob data not present");
            // Prepare batch
            batch = std::make_shared<NArrow::TGeneralContainer>(NArrow::DeserializeBatch(blobData, indexInfo.ArrowSchema()));
            AFL_VERIFY(batch)("event", "cannot_parse")("data_snapshot", inserted.GetSnapshot())("index_snapshot", blobSchema->GetSnapshot());
        }

        AddSpecials(*batch, indexInfo, inserted);
        batch = resultSchema->NormalizeBatch(*blobSchema, batch).DetachResult();
        pathBatches.Add(inserted, shardingFilterCommit, batch);
    }

    Y_ABORT_UNLESS(Blobs.IsEmpty());
    const std::vector<std::string> comparableColumns = resultSchema->GetIndexInfo().GetReplaceKey()->field_names();
    for (auto& [pathId, pathInfo] : pathBatches.GetData()) {
        auto shardingFilter = context.SchemaVersions.GetShardingInfoActual(pathId);
        auto mergedBatch = pathInfo.Merge(resultSchema->GetIndexInfo());
        Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(mergedBatch, resultSchema->GetIndexInfo().GetReplaceKey()));

        auto itGranule = PathToGranule.find(pathId);
        AFL_VERIFY(itGranule != PathToGranule.end());
        std::vector<std::shared_ptr<arrow::RecordBatch>> result =
            NArrow::NMerger::TRWSortableBatchPosition::SplitByBordersInSequentialContainer(mergedBatch, comparableColumns, itGranule->second);
        for (auto&& b : result) {
            if (!b) {
                continue;
            }
            std::optional<NArrow::NSerialization::TSerializerContainer> externalSaver;
            if (b->num_rows() < 100) {
                externalSaver = NArrow::NSerialization::TSerializerContainer(
                    std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::UNCOMPRESSED));
            } else {
                externalSaver = NArrow::NSerialization::TSerializerContainer(
                    std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::LZ4_FRAME));
            }
            auto portions = MakeAppendedPortions(b, pathId, maxSnapshot, nullptr, context, externalSaver);
            Y_ABORT_UNLESS(portions.size());
            for (auto& portion : portions) {
                if (pathInfo.GetShardingInfo()) {
                    portion.GetPortionConstructor().SetShardingVersion(pathInfo.GetShardingInfo()->GetSnapshotVersion());
                }
                AppendedPortions.emplace_back(TWritePortionInfoWithBlobsResult(std::move(portion)));
            }
        }
    }

    Y_ABORT_UNLESS(PathToGranule.size() == pathBatches.GetData().size());
    return TConclusionStatus::Success();
}

void TInsertColumnEngineChanges::AddSpecials(
    NArrow::TGeneralContainer& batch, const TIndexInfo& indexInfo, const TInsertedData& inserted) const {
    IIndexInfo::AddSnapshotColumns(batch, inserted.GetSnapshot());
    IIndexInfo::AddDeleteFlagsColumn(batch, inserted.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete);
}

NColumnShard::ECumulativeCounters TInsertColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL;
}

}
