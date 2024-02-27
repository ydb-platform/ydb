#include "tx_draft.h"

namespace NKikimr::NColumnShard {

bool TTxWriteDraft::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    for (auto&& action : WriteController->GetBlobActions()) {
        action->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
    }
    return true;
}

void TTxWriteDraft::Complete(const TActorContext& ctx) {
    for (auto&& action : WriteController->GetBlobActions()) {
        action->OnCompleteTxBeforeWrite(*Self);
    }
    ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
}

}
