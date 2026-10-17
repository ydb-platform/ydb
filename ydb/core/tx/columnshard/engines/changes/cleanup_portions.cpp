#include "cleanup_portions.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

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
    AFL_VERIFY(FetchedDataAccessors);
    PortionsToRemove.ApplyOnExecute(self, context, *FetchedDataAccessors);

    if (!self) {
        return;
    }
    NIceDb::TNiceDb db(*context.DB);
    for (const auto& [pathId, snapshots] : TruncatesToRemove) {
        self->TablesManager.RemoveTruncateSnapshotsOnExecute(pathId, snapshots, db);
    }

    THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;

    for (auto&& p : PortionsToDrop) {
        const auto& accessor = FetchedDataAccessors->GetPortionAccessorVerified(p->GetPortionId());
        accessor.RemoveFromDatabase(context.DBWrapper);
        accessor.FillBlobIdsByStorage(blobIdsByStorage, context.EngineLogs.GetVersionedIndex());
    }
    for (auto&& i : blobIdsByStorage) {
        auto action = BlobsAction.GetRemoving(i.first);
        for (auto&& b : i.second) {
            action->DeclareRemove((TTabletId)self->TabletID(), b);
        }
    }

    if (PortionsToDrop.size() && self->LastCleanupSnapshot < MinSnapshotForNewReads) {
        self->LastCleanupSnapshot = MinSnapshotForNewReads;
        NColumnShard::Schema::SaveSpecialValue(
            db, NColumnShard::Schema::EValueIds::LastCleanupSnapshotStep, self->LastCleanupSnapshot.GetPlanStep());
        NColumnShard::Schema::SaveSpecialValue(
            db, NColumnShard::Schema::EValueIds::LastCleanupSnapshotTxId, self->LastCleanupSnapshot.GetTxId());
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    PortionsToRemove.ApplyOnComplete(self, context, *FetchedDataAccessors);
    for (auto& portionInfo : PortionsToDrop) {
        if (!context.EngineLogs.ErasePortion(*portionInfo)) {
            YDB_LOG_WARN("",
                {"event", "Cannot erase portion"},
                {"portion", portionInfo->DebugString()});
        }
    }
    if (self) {
        for (const auto& [pathId, snapshots] : TruncatesToRemove) {
            self->TablesManager.RemoveTruncateSnapshotsOnComplete(pathId, snapshots);
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

void TCleanupPortionsColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    if (!context.FinishedSuccessfully) {
        auto& engine = self.MutableIndexAs<TColumnEngineForLogs>();
        for (const auto& portion : PortionsToDrop) {
            engine.AddCleanupPortion(portion);
        }
        for (const auto& [pathId, _] : TruncatesToRemove) {
            // An aborted task has not changed canonical history; restore only GC indexes.
            engine.ApplyTruncateSnapshots(pathId);
        }
    }
    self.BackgroundController.FinishCleanupPortions();
}

NColumnShard::ECumulativeCounters TCleanupPortionsColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}   // namespace NKikimr::NOlap
