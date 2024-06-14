#include "tx_remove_blobs.h"
#include <ydb/core/tx/columnshard/blobs_action/events/delete_blobs.h>

namespace NKikimr::NColumnShard {

bool TTxRemoveSharedBlobs::Execute(TTransactionContext& txc, const TActorContext&) {
    TMemoryProfileGuard mpg("TTxRemoveSharedBlobs::Execute");
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "execute");
    NOlap::TBlobManagerDb blobManagerDb(txc.DB);
    RemoveAction->OnExecuteTxAfterRemoving(blobManagerDb, true);

    Manager->RemoveSharedBlobsDB(txc, SharingBlobIds);
    return true;
}

void TTxRemoveSharedBlobs::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxRemoveSharedBlobs::Complete");
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("tablet_id", Self->TabletID())("tx_state", "complete");
    RemoveAction->OnCompleteTxAfterRemoving(true);
    Manager->RemoveSharedBlobs(SharingBlobIds);

    ctx.Send(InitiatorActorId, new NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobsFinished((NOlap::TTabletId)Self->TabletID(),
        NKikimrColumnShardBlobOperationsProto::TEvDeleteSharedBlobsFinished::Success));

    Self->GetStoragesManager()->GetSharedBlobsManager()->FinishExternalModification();
}

}
