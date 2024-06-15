#include "tx_change_blobs_owning.h"
#include <ydb/core/tx/columnshard/data_sharing/modification/events/change_owning.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxApplyLinksModification::DoExecute(TTransactionContext& txc, const TActorContext&) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "execute");
    Task->ApplyForDB(txc, Self->GetStoragesManager()->GetSharedBlobsManager());
    return true;
}

void TTxApplyLinksModification::DoComplete(const TActorContext& /*ctx*/) {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "complete");
    Task->ApplyForRuntime(Self->GetStoragesManager()->GetSharedBlobsManager());

    auto ev = std::make_unique<NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished>(Task->GetTabletId(), SessionId, PackIdx);
    NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(ev.release(), (ui64)InitiatorTabletId, true), IEventHandle::FlagTrackDelivery);
}

}
