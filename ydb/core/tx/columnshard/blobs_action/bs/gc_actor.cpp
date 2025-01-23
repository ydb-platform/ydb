#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TGarbageCollectionActor::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev) {
    PendingGroupReplies--;
    ACFL_DEBUG("actor", "TEvCollectGarbageResult");
    if (ev->Get()->Status == NKikimrProto::BLOCKED) {
        auto g = PassAwayGuard();
        ACFL_WARN("event", "blocked_gc_event");
    } else if (ev->Get()->Status == NKikimrProto::OK) {
        GCTask->OnGCResult(ev);
        CheckFinished();
    } else {
        ACFL_ERROR()("event", "GC_ERROR")("details", ev->Get()->Print(true));
        if (auto gcEvent = GCTask->BuildRequest(TBlobAddress(ev->Cookie, ev->Get()->Channel))) {
            SendToBSProxy(NActors::TActivationContext::AsActorContext(), ev->Cookie, gcEvent.release(), ev->Cookie);
        } else {
            Become(&TGarbageCollectionActor::StateDying);
        }
    }
}

void TGarbageCollectionActor::HandleOnDying(TEvBlobStorage::TEvCollectGarbageResult::TPtr& /*ev*/) {
    PendingGroupReplies--;
    CheckReadyToDie();
}


void TGarbageCollectionActor::CheckFinished() {
    if (SharedRemovingFinished && GCTask->IsFinished()) {
        auto g = PassAwayGuard();
        ACFL_DEBUG("actor", "TGarbageCollectionActor")("event", "finished");
        TActorContext::AsActorContext().Send(TabletActorId, std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(GCTask));
    }
}

void TGarbageCollectionActor::CheckReadyToDie() {
    if (PendingGroupReplies == 0) {
        auto guard = PassAwayGuard();
        Send(TabletActorId, new TEvents::TEvPoison);
    }
}


}
