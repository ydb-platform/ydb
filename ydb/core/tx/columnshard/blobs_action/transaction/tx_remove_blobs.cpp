#include "tx_remove_blobs.h"
#include <ydb/core/tx/columnshard/blobs_action/events/delete_blobs.h>

namespace NKikimr::NColumnShard {

bool TTxRemoveSharedBlobs::Execute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    RemoveAction->OnExecuteTxAfterRemoving(*Self, blobManagerDb, true);
    return true;
}

void TTxRemoveSharedBlobs::Complete(const TActorContext& ctx) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "complete");
    RemoveAction->OnCompleteTxAfterRemoving(*Self, true);

    ctx.Send(InitiatorActorId, new NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobsFinished((NOlap::TTabletId)Self->TabletID()));
}

}
