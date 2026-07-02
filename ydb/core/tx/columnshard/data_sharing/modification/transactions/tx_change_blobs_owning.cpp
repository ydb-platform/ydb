#include "tx_change_blobs_owning.h"

#include <ydb/core/tx/columnshard/data_sharing/modification/events/change_owning.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxApplyLinksModification::DoExecute(TTransactionContext& txc, const TActorContext&) {
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"tabletId", Self->TabletID()},
        {"txState", "execute"});
    Task->ApplyForDB(txc, Self->GetStoragesManager()->GetSharedBlobsManager());
    return true;
}

void TTxApplyLinksModification::DoComplete(const TActorContext& /*ctx*/) {
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"tabletId", Self->TabletID()},
        {"txState", "complete"});
    Task->ApplyForRuntime(Self->GetStoragesManager()->GetSharedBlobsManager());

    auto ev = std::make_unique<NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished>(Task->GetTabletId(), SessionId, PackIdx);
    NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(ev.release(), (ui64)InitiatorTabletId, true), IEventHandle::FlagTrackDelivery);
}

}   // namespace NKikimr::NOlap::NDataSharing
