#include "cleanup.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

void TCleanupColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo.DebugString();
        }
    }
}

void TCleanupColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    THashSet<ui64> pathIds;
    if (self) {
        THashMap<TString, THashSet<TUnifiedBlobId>> blobIdsByStorage;
        for (auto&& p : PortionsToDrop) {
            p.RemoveFromDatabase(context.DBWrapper);

            p.FillBlobIdsByStorage(blobIdsByStorage, context.EngineLogs.GetVersionedIndex());
            pathIds.emplace(p.GetPathId());
        }
        for (auto&& i : blobIdsByStorage) {
            auto action = BlobsAction.GetRemoving(i.first);
            for (auto&& b : i.second) {
                action->DeclareRemove((TTabletId)self->TabletID(), b);
            }
        }

        if (context.DB) {
            for (auto&& p : pathIds) {
                self->TablesManager.TryFinalizeDropPath(*context.DB, p);
            }
        }
    }
}

void TCleanupColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    for (auto& portionInfo : PortionsToDrop) {
        if (!context.EngineLogs.ErasePortion(portionInfo)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo.DebugString());
        }
    }
    context.TriggerActivity = NeedRepeat ? NColumnShard::TBackgroundActivity::Cleanup() : NColumnShard::TBackgroundActivity::None();
    if (self) {
        self->IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
        for (auto&& p : PortionsToDrop) {
            self->IncCounter(NColumnShard::COUNTER_RAW_BYTES_ERASED, p.RawBytesSum());
        }
    }
}

void TCleanupColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanup();
}

void TCleanupColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanup();
}

NColumnShard::ECumulativeCounters TCleanupColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}
