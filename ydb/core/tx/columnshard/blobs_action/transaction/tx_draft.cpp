#include "tx_draft.h"

namespace NKikimr::NColumnShard {

bool TTxWriteDraft::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Execute");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "draft_started");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "draft_finished");
    return true;
}

void TTxWriteDraft::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Complete");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_WRITE)("event", "draft_completed");
    Completed = true;
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnCompleteTxBeforeWrite(*Self);
    }
    ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
}

}
