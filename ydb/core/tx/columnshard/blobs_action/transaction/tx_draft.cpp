#include "tx_draft.h"

namespace NKikimr::NColumnShard {

bool TTxWriteDraft::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
    }
    return true;
}

void TTxWriteDraft::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxWriteDraft::Complete");
    Completed = true;
    for (auto&& action : WriteController->GetBlobActions()) {
        action.second->OnCompleteTxBeforeWrite(*Self);
    }
    ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
}

}
