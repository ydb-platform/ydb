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
    const auto predRemoveDroppedTable = [self](const TWritePortionInfoWithBlobsResult& item) {
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
                    self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_INDEXING_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::COMPACTED:
                    self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_COMPACTION_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::SPLIT_COMPACTED:
                    self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_PORTIONS_WRITTEN);
                    break;
                case NOlap::TPortionMeta::EProduced::EVICTED:
                    Y_ABORT("Unexpected evicted case");
                    break;
                case NOlap::TPortionMeta::EProduced::INACTIVE:
                    Y_ABORT("Unexpected inactive case");
                    break;
            }
        }
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, PortionsToRemove.size());

        THashSet<TUnifiedBlobId> blobsDeactivated;
        for (auto& [_, portionInfo] : PortionsToRemove) {
            for (auto& rec : portionInfo.Records) {
                blobsDeactivated.emplace(portionInfo.GetBlobId(rec.BlobRange.GetBlobIdxVerified()));
            }
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo.GetTotalRawBytes());
        }

        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, blobsDeactivated.size());
        for (auto& blobId : blobsDeactivated) {
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
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

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& /*self*/) {
}

}   // namespace NKikimr::NOlap
