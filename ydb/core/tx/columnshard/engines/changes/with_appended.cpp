#include "with_appended.h"

#include "counters/general.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

namespace NKikimr::NOlap {

void TChangesWithAppend::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    THashSet<ui64> usedPortionIds;
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();

    for (auto&& [_, i] : PortionsToRemove) {
        Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
        AFL_VERIFY(usedPortionIds.emplace(i->GetPortionId()).second)("portion_info", i->DebugString(true));
        const auto pred = [&](TPortionInfo& portionCopy) {
            portionCopy.SetRemoveSnapshot(context.Snapshot);
        };
        context.EngineLogs.GetGranuleVerified(i->GetPathId())
            .ModifyPortionOnExecute(
                context.DBWrapper, GetPortionDataAccessor(i->GetPortionId()), pred, schemaPtr->GetIndexInfo().GetPKFirstColumnId());
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
    for (auto&& [_, i] : PortionsToMove) {
        const auto pred = [&](TPortionInfo& portionCopy) {
            portionCopy.MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
        };
        context.EngineLogs.GetGranuleVerified(i->GetPathId())
            .ModifyPortionOnExecute(
                context.DBWrapper, GetPortionDataAccessor(i->GetPortionId()), pred, schemaPtr->GetIndexInfo().GetPKFirstColumnId());
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
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("portions", sb)("task_id", GetTaskIdentifier());
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, PortionsToRemove.size());

        for (auto& [_, portionInfo] : PortionsToRemove) {
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, portionInfo->GetBlobIdsCount());
            for (auto& blobId : portionInfo->GetBlobIds()) {
                self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
            }
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo->GetTotalRawBytes());
        }

    }
    if (PortionsToMove.size()) {
        THashMap<ui32, TSimplePortionsGroupInfo> portionGroups;
        for (auto&& [_, i] : PortionsToMove) {
            portionGroups[i->GetMeta().GetCompactionLevel()].AddPortion(i);
        }
        NChanges::TGeneralCompactionCounters::OnMovePortionsByLevel(portionGroups, TargetCompactionLevel.value_or(0));
        for (auto&& [_, i] : PortionsToMove) {
            const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
                portion->MutableMeta().ResetCompactionLevel(TargetCompactionLevel.value_or(0));
            };
            context.EngineLogs.ModifyPortionOnComplete(i, pred);
        }
    }
    {
        auto g = context.EngineLogs.GranulesStorage->GetStats()->StartPackModification();
        for (auto&& [_, i] : PortionsToRemove) {
            Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
            const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
                portion->SetRemoveSnapshot(context.Snapshot);
            };
            context.EngineLogs.ModifyPortionOnComplete(i, pred);
            context.EngineLogs.AddCleanupPortion(i);
        }
        for (auto& portionBuilder : AppendedPortions) {
            context.EngineLogs.AppendPortion(portionBuilder.GetPortionResult());
        }
    }
}

void TChangesWithAppend::DoCompile(TFinalizationContext& context) {
    AFL_VERIFY(PortionsToRemove.size() + PortionsToMove.size() + AppendedPortions.size());
    for (auto&& i : AppendedPortions) {
        i.GetPortionConstructor().MutablePortionConstructor().SetPortionId(context.NextPortionId());
        i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(TargetCompactionLevel.value_or(0));
    }
}

void TChangesWithAppend::DoOnAfterCompile() {
    if (AppendedPortions.size()) {
        for (auto&& i : AppendedPortions) {
            i.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetCompactionLevel(TargetCompactionLevel.value_or(0));
            i.FinalizePortionConstructor();
        }
    }
}

void TChangesWithAppend::DoStart(NColumnShard::TColumnShard& /*self*/) {
}

}   // namespace NKikimr::NOlap
