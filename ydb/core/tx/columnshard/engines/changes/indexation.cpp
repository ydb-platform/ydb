#include "indexation.h"

#include "compaction/merger.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

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

    std::vector<std::shared_ptr<NArrow::TGeneralContainer>> GetGeneralContainers() const {
        std::vector<std::shared_ptr<NArrow::TGeneralContainer>> result;
        for (auto&& i : Batches) {
            result.emplace_back(i.GetBatch());
        }
        return result;
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
    std::set<ui32> usageColumnIds;
    {
        THashMap<ui64, ISnapshotSchema::TPtr> schemas;
        for (auto& inserted : DataToIndex) {
            if (schemas.contains(inserted.GetSchemaVersion())) {
                continue;
            }
            schemas.emplace(inserted.GetSchemaVersion(), context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion()));
        }
        usageColumnIds = ISnapshotSchema::GetColumnsWithDifferentDefaults(schemas, resultSchema);
    }

    for (auto& inserted : DataToIndex) {
        auto blobSchema = context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion());
        std::vector<ui32> filteredIds = inserted.GetMeta().GetSchemaSubset().Apply(blobSchema->GetIndexInfo().GetColumnIds(false));
        usageColumnIds.insert(filteredIds.begin(), filteredIds.end());
        if (inserted.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete) {
            usageColumnIds.emplace((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG);
        }
        if (usageColumnIds.size() == resultSchema->GetIndexInfo().GetColumnIds(true).size()) {
            break;
        }
    }

    for (auto& inserted : DataToIndex) {
        const TBlobRange& blobRange = inserted.GetBlobRange();
        auto shardingFilterCommit = context.SchemaVersions.GetShardingInfoOptional(inserted.PathId, inserted.GetSnapshot());
        auto blobSchema = context.SchemaVersions.GetSchemaVerified(inserted.GetSchemaVersion());

        std::shared_ptr<NArrow::TGeneralContainer> batch;
        {
            const auto blobData = Blobs.Extract(IStoragesManager::DefaultStorageId, blobRange);
            auto batchSchema =
                std::make_shared<arrow::Schema>(inserted.GetMeta().GetSchemaSubset().Apply(blobSchema->GetIndexInfo().ArrowSchema()->fields()));
            batch = std::make_shared<NArrow::TGeneralContainer>(NArrow::DeserializeBatch(blobData, batchSchema));
        }

        IIndexInfo::AddSnapshotColumns(*batch, inserted.GetSnapshot());
        if (usageColumnIds.contains((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
            IIndexInfo::AddDeleteFlagsColumn(*batch, inserted.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete);
        }
        usageColumnIds.insert(IIndexInfo::GetSnapshotColumnIds().begin(), IIndexInfo::GetSnapshotColumnIds().end());

        batch = resultSchema->NormalizeBatch(*blobSchema, batch, usageColumnIds).DetachResult();
        pathBatches.Add(inserted, shardingFilterCommit, batch);
    }

    Y_ABORT_UNLESS(Blobs.IsEmpty());
    auto filteredSnapshot = std::make_shared<TFilteredSnapshotSchema>(resultSchema, usageColumnIds);
    auto stats = std::make_shared<TSerializationStats>();
    std::vector<std::shared_ptr<NArrow::TColumnFilter>> filters;
    for (auto& [pathId, pathInfo] : pathBatches.GetData()) {
        std::optional<ui64> shardingVersion;
        if (pathInfo.GetShardingInfo()) {
            shardingVersion = pathInfo.GetShardingInfo()->GetSnapshotVersion();
        }
        auto batches = pathInfo.GetGeneralContainers();
        filters.resize(batches.size());

        auto itGranule = PathToGranule.find(pathId);
        AFL_VERIFY(itGranule != PathToGranule.end());
        NCompaction::TMerger merger(context, SaverContext, std::move(batches), std::move(filters));
        merger.SetOptimizationWritingPackMode(true);
        auto localAppended = merger.Execute(stats, itGranule->second, filteredSnapshot, pathId, shardingVersion);
        for (auto&& i : localAppended) {
            AppendedPortions.emplace_back(std::move(i));
        }
    }

    Y_ABORT_UNLESS(PathToGranule.size() == pathBatches.GetData().size());
    return TConclusionStatus::Success();
}

NColumnShard::ECumulativeCounters TInsertColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_INDEXING_SUCCESS : NColumnShard::COUNTER_INDEXING_FAIL;
}

}
