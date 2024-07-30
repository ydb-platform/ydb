#include "cleanup_portions.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

void TCleanupPortionsColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo.DebugString();
        }
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
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
    }
}

void TCleanupPortionsColumnEngineChanges::DoWriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    for (auto& portionInfo : PortionsToDrop) {
        if (!context.EngineLogs.ErasePortion(portionInfo)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo.DebugString());
        }
    }
    if (self) {
        self->Counters.GetTabletCounters().IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
        for (auto&& p : PortionsToDrop) {
            self->Counters.GetTabletCounters().IncCounter(NColumnShard::COUNTER_RAW_BYTES_ERASED, p.GetTotalRawBytes());
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

}
