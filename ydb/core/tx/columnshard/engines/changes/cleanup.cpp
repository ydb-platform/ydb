#include "cleanup.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap {

void TCleanupColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo->DebugString();
        }
    }
}

void TCleanupColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& /*context*/) {
    self.IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
    THashSet<TUnifiedBlobId> blobIds;
    for (auto&& p : PortionsToDrop) {
        auto removing = BlobsAction.GetRemoving(*p);
        for (auto&& r : p->Records) {
            removing->DeclareRemove(r.BlobRange.BlobId);
        }
        self.IncCounter(NColumnShard::COUNTER_RAW_BYTES_ERASED, p->RawBytesSum());
    }
}

bool TCleanupColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    THashSet<TUnifiedBlobId> blobIds;
    for (auto& portionInfo : PortionsToDrop) {
        if (!self.ErasePortion(*portionInfo)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo->DebugString());
            return false;
        }
        for (auto& record : portionInfo->Records) {
            self.ColumnsTable->Erase(context.DB, *portionInfo, record);
        }
    }

    return true;
}

void TCleanupColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanup();
}

void TCleanupColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& /*self*/, TWriteIndexCompleteContext& context) {
    context.TriggerActivity = NeedRepeat ? NColumnShard::TBackgroundActivity::Cleanup() : NColumnShard::TBackgroundActivity::None();
}

void TCleanupColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanup();
}

NColumnShard::ECumulativeCounters TCleanupColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}
