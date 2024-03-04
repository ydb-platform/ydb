#include "gc.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap {

void IBlobsGCAction::OnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (!AbortedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnCompleteTxAfterCleaning")("action_guid", GetActionGuid());
        if (!DoOnCompleteTxAfterCleaning(self, taskAction)) {
            return;
        }
        taskAction->OnFinished();
    }
}

void IBlobsGCAction::OnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
    if (!AbortedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnExecuteTxAfterCleaning")("action_guid", GetActionGuid());
        return DoOnExecuteTxAfterCleaning(self, dbBlobs);
    }
}

void IBlobsGCAction::Abort() {
    Y_ABORT_UNLESS(IsInProgress());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "gc_aborted")("action_guid", GetActionGuid());
    AbortedFlag = true;
}

void IBlobsGCAction::OnFinished() {
    Y_ABORT_UNLESS(IsInProgress());
    FinishedFlag = true;
}

}
