#include "with_appended.h"

#include "counters/general.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

namespace NKikimr::NOlap {

void TChangesWithAppend::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    THashSet<ui64> usedPortionIds = PortionsToRemove.GetPortionIds();
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
    if (PortionsToRemove.GetSize() || PortionsToMove.GetSize()) {
        AFL_VERIFY(FetchedDataAccessors);
        PortionsToRemove.ApplyOnExecute(self, context, *FetchedDataAccessors);
        PortionsToMove.ApplyOnExecute(self, context, *FetchedDataAccessors);
    }
    const auto predRemoveDroppedTable = [self](const TWritePortionInfoWithBlobsResult& item) {
        auto& portionInfo = item.GetPortionResult();
        if (!!self && !self->TablesManager.HasTable(portionInfo.GetPortionInfo().GetPathId(), false)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_inserted_data")("reason", "table_removed")(
                "path_id", portionInfo.GetPortionInfo().GetPathId());
            return true;
        } else {
            return false;
        }
    };
    AppendedPortions.erase(std::remove_if(AppendedPortions.begin(), AppendedPortions.end(), predRemoveDroppedTable), AppendedPortions.end());
    for (auto& portionInfoWithBlobs : AppendedPortions) {
        const auto& portionInfo = portionInfoWithBlobs.GetPortionResult().GetPortionInfoPtr();
        AFL_VERIFY(usedPortionIds.emplace(portionInfo->GetPortionId()).second)("portion_info", portionInfo->DebugString(true));
        portionInfoWithBlobs.GetPortionResult().SaveToDatabase(context.DBWrapper, schemaPtr->GetIndexInfo().GetPKFirstColumnId(), false);
    }
}

void TChangesWithAppend::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    if (self) {
        TStringBuilder sb;
        for (auto& portionBuilder : AppendedPortions) {
            auto& portionInfo = portionBuilder.GetPortionResult();
            sb << portionInfo.GetPortionInfo().GetPortionId() << ",";
            switch (portionInfo.GetPortionInfo().GetMeta().Produced) {
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("portions", sb)("task_id", GetTaskIdentifier());
    }

    auto g = context.EngineLogs.GranulesStorage->GetStats()->StartPackModification();
    if (PortionsToRemove.GetSize() || PortionsToMove.GetSize()) {
        PortionsToRemove.ApplyOnComplete(self, context, *FetchedDataAccessors);
        PortionsToMove.ApplyOnComplete(self, context, *FetchedDataAccessors);
    }
    for (auto& portionBuilder : AppendedPortions) {
        context.EngineLogs.AppendPortion(portionBuilder.GetPortionResult());
    }

}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    AFL_VERIFY(PortionsToRemove.GetSize() + PortionsToMove.GetSize() + AppendedPortions.size() || NoAppendIsCorrect);
    for (auto&& i : AppendedPortions) {
        i.GetPortionConstructor().MutablePortionConstructor().SetPortionId(context.NextPortionId());
        i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(PortionsToMove.GetTargetCompactionLevel().value_or(0));
    }
}

void TChangesWithAppend::DoOnAfterCompile() {
    for (auto&& i : AppendedPortions) {
        i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(
            PortionsToMove.GetTargetCompactionLevel().value_or(0));
        i.FinalizePortionConstructor();
    }
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& /*self*/) {
}

}   // namespace NKikimr::NOlap
