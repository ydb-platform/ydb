#include "cleanup_portions.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap {

void TCleanupPortionsColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo->DebugString();
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    THashSet<ui64> pathIds;
    if (!self) {
        return;
    }
    THashSet<ui64> usedPortionIds;
    auto schemaPtr = context.EngineLogs.GetVersionedIndex().GetLastSchema();
    for (auto&& i : PortionsToRemove) {
        Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
        AFL_VERIFY(usedPortionIds.emplace(i->GetPortionId()).second)("portion_info", i->DebugString(true));
        const auto pred = [&](TPortionInfo& portionCopy) {
            portionCopy.SetRemoveSnapshot(context.Snapshot);
        };
        context.EngineLogs.GetGranuleVerified(i->GetPathId())
            .ModifyPortionOnExecute(
                context.DBWrapper, GetPortionDataAccessor(i->GetPortionId()), pred, schemaPtr->GetIndexInfo().GetPKFirstColumnId());
    }

    THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;
    for (auto&& [_, p] : FetchedDataAccessors->GetPortions()) {
        p.RemoveFromDatabase(context.DBWrapper);
        p.FillBlobIdsByStorage(blobIdsByStorage, context.EngineLogs.GetVersionedIndex());
        pathIds.emplace(p.GetPortionInfo().GetPathId());
    }
    for (auto&& i : blobIdsByStorage) {
        auto action = BlobsAction.GetRemoving(i.first);
        for (auto&& b : i.second) {
            action->DeclareRemove((TTabletId)self->TabletID(), b);
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    {
        auto g = context.EngineLogs.GranulesStorage->GetStats()->StartPackModification();
        for (auto&& i : PortionsToRemove) {
            Y_ABORT_UNLESS(!i->HasRemoveSnapshot());
            const auto pred = [&](const std::shared_ptr<TPortionInfo>& portion) {
                portion->SetRemoveSnapshot(context.Snapshot);
            };
            context.EngineLogs.ModifyPortionOnComplete(i, pred);
            context.EngineLogs.AddCleanupPortion(i);
        }
    }
    for (auto& portionInfo : PortionsToDrop) {
        if (!context.EngineLogs.ErasePortion(*portionInfo)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo->DebugString());
        }
    }
    if (self) {
        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_DEACTIVATED, PortionsToRemove.size());
        for (auto& portionInfo : PortionsToRemove) {
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BLOBS_DEACTIVATED, portionInfo->GetBlobIdsCount());
            for (auto& blobId : portionInfo->GetBlobIds()) {
                self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
            }
            self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_RAW_BYTES_DEACTIVATED, portionInfo->GetTotalRawBytes());
        }

        self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
        for (auto&& p : PortionsToDrop) {
            self->Counters.GetTabletCounters()->OnDropPortionEvent(p->GetTotalRawBytes(), p->GetTotalBlobBytes(), p->GetRecordsCount());
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanupPortions();
}

void TCleanupPortionsColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanupPortions();
}

NColumnShard::ECumulativeCounters TCleanupPortionsColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}   // namespace NKikimr::NOlap
