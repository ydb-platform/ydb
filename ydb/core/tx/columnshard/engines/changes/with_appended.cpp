#include "with_appended.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

namespace NKikimr::NOlap {

void TChangesWithAppend::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    THashSet<ui64> usedPortionIds;
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
    for (auto& [_, portionInfo] : PortionsToRemove) {
        Y_ABORT_UNLESS(!portionInfo.Empty());
        Y_ABORT_UNLESS(portionInfo.HasRemoveSnapshot());
        AFL_VERIFY(usedPortionIds.emplace(portionInfo.GetPortionId()).second)("portion_info", portionInfo.DebugString(true));
        portionInfo.SaveToDatabase(context.DBWrapper, schemaPtr->GetIndexInfo().GetPKFirstColumnId(), false);
    }
    const auto predRemoveDroppedTable = [self](const TWritePortionInfoWithBlobs& item) {
        auto& portionInfo = item.GetPortionResult();
        if (!!self && (!self->TablesManager.HasTable(portionInfo.GetPathId()) || self->TablesManager.GetTable(portionInfo.GetPathId()).IsDropped())) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_inserted_data")("reason", "table_removed")("path_id", portionInfo.GetPathId());
            return true;
        } else {
            return false;
        }
    };
    AppendedPortions.erase(std::remove_if(AppendedPortions.begin(), AppendedPortions.end(), predRemoveDroppedTable), AppendedPortions.end());
    for (auto& portionInfoWithBlobs : AppendedPortions) {
        auto& portionInfo = portionInfoWithBlobs.GetPortionResult();
        AFL_VERIFY(usedPortionIds.emplace(portionInfo.GetPortionId()).second)("portion_info", portionInfo.DebugString(true));
        portionInfo.SaveToDatabase(context.DBWrapper, schemaPtr->GetIndexInfo().GetPKFirstColumnId(), false);
    }
}

void TChangesWithAppend::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    if (self) {
        for (auto& portionBuilder : AppendedPortions) {
            auto& portionInfo = portionBuilder.GetPortionResult();
            switch (portionInfo.GetMeta().Produced) {
                case NOlap::TPortionMeta::EProduced::UNSPECIFIED:
                    Y_ABORT_UNLESS(false);   // unexpected
                case NOlap::TPortionMeta::EProduced::INSERTED:
                    self->IncCounter(NColumnShard::COUNTER_INDEXING_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::COMPACTED:
                    self->IncCounter(NColumnShard::COUNTER_COMPACTION_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::SPLIT_COMPACTED:
                    self->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::EVICTED:
                    Y_ABORT("Unexpected evicted case");
                    break;
                case NOlap::TPortionMeta::EProduced::INACTIVE:
                    Y_ABORT("Unexpected inactive case");
                    break;
            }
        }
        self->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, PortionsToRemove.size());

        THashSet<TUnifiedBlobId> blobsDeactivated;
        for (auto& [_, portionInfo] : PortionsToRemove) {
            for (auto& rec : portionInfo.Records) {
                blobsDeactivated.emplace(portionInfo.GetBlobId(rec.BlobRange.GetBlobIdxVerified()));
            }
            self->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo.GetTotalRawBytes());
        }

        self->IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, blobsDeactivated.size());
        for (auto& blobId : blobsDeactivated) {
            self->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
        }
    }
    {
        auto g = context.EngineLogs.GranulesStorage->GetStats()->StartPackModification();
        for (auto& [_, portionInfo] : PortionsToRemove) {
            context.EngineLogs.AddCleanupPortion(portionInfo);
            const TPortionInfo& oldInfo = context.EngineLogs.GetGranuleVerified(portionInfo.GetPathId()).GetPortionVerified(portionInfo.GetPortion());
            context.EngineLogs.UpsertPortion(portionInfo, &oldInfo);
        }
        for (auto& portionBuilder : AppendedPortions) {
            context.EngineLogs.UpsertPortion(portionBuilder.GetPortionResult());
        }
    }
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    for (auto&& i : AppendedPortions) {
        i.GetPortionConstructor().SetPortionId(context.NextPortionId());
        i.GetPortionConstructor().MutableMeta().UpdateRecordsMeta(TPortionMeta::EProduced::INSERTED);
    }
    for (auto& [_, portionInfo] : PortionsToRemove) {
        portionInfo.SetRemoveSnapshot(context.GetSnapshot());
    }
}

void TChangesWithAppend::DoOnAfterCompile() {
    for (auto&& i : AppendedPortions) {
        i.FinalizePortionConstructor();
    }
}

std::vector<TWritePortionInfoWithBlobs> TChangesWithAppend::MakeAppendedPortions(const std::shared_ptr<arrow::RecordBatch> batch,
    const ui64 pathId, const TSnapshot& snapshot, const TGranuleMeta* granuleMeta, TConstructionContext& context, const std::optional<NArrow::NSerialization::TSerializerContainer>& overrideSaver) const {
    Y_ABORT_UNLESS(batch->num_rows());

    auto resultSchema = context.SchemaVersions.GetSchema(snapshot);

    std::shared_ptr<NOlap::TSerializationStats> stats = std::make_shared<NOlap::TSerializationStats>();
    if (granuleMeta) {
        stats = granuleMeta->BuildSerializationStats(resultSchema);
    }
    auto schema = std::make_shared<TDefaultSchemaDetails>(resultSchema, stats);
    if (overrideSaver) {
        schema->SetOverrideSerializer(*overrideSaver);
    }
    std::vector<TWritePortionInfoWithBlobs> out;
    {
        std::vector<TBatchSerializedSlice> pages = TBatchSerializedSlice::BuildSimpleSlices(batch, NSplitter::TSplitSettings(), context.Counters.SplitterCounters, schema);
        std::vector<TGeneralSerializedSlice> generalPages;
        for (auto&& i : pages) {
            auto portionColumns = i.GetPortionChunksToHash();
            resultSchema->GetIndexInfo().AppendIndexes(portionColumns);
            generalPages.emplace_back(portionColumns, schema, context.Counters.SplitterCounters);
        }

        const NSplitter::TEntityGroups groups = resultSchema->GetIndexInfo().GetEntityGroupsByStorageId(IStoragesManager::DefaultStorageId, *SaverContext.GetStoragesManager());
        TSimilarPacker slicer(NSplitter::TSplitSettings().GetExpectedPortionSize());
        auto packs = slicer.Split(generalPages);

        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slice(std::move(i));
            auto b = batch->Slice(recordIdx, slice.GetRecordsCount());
            out.emplace_back(TWritePortionInfoWithBlobs::BuildByBlobs(slice.GroupChunksByBlobs(groups), pathId, resultSchema->GetVersion(), snapshot, SaverContext.GetStoragesManager()));
            out.back().FillStatistics(resultSchema->GetIndexInfo());
            out.back().GetPortionConstructor().AddMetadata(*resultSchema, b);
            out.back().GetPortionConstructor().MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
            recordIdx += slice.GetRecordsCount();
        }
    }

    return out;
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& /*self*/) {
}

}   // namespace NKikimr::NOlap
