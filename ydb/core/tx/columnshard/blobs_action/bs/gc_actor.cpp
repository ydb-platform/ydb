#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TGarbageCollectionActor::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev) {
    ACFL_DEBUG("actor", "TEvCollectGarbageResult");
    if (ev->Get()->Status == NKikimrProto::BLOCKED) {
        auto g = PassAwayGuard();
        ACFL_WARN("event", "blocked_gc_event");
        return;
    } else if (ev->Get()->Status == NKikimrProto::OK) {
        GCTask->OnGCResult(ev);
        CheckFinished();
    } else {
        ACFL_ERROR()("event", "GC_ERROR")("details", ev->Get()->Print(true));
        SendToBSProxy(NActors::TActivationContext::AsActorContext(), ev->Cookie, GCTask->BuildRequest(TBlobAddress(ev->Cookie, ev->Get()->Channel)).release(), ev->Cookie);
    }
}

void TGarbageCollectionActor::CheckFinished() {
    if (SharedRemovingFinished && GCTask->IsFinished()) {
        auto g = PassAwayGuard();
        ACFL_DEBUG("actor", "TGarbageCollectionActor")("event", "finished");
        TActorContext::AsActorContext().Send(TabletActorId, std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(GCTask));
    }
}

}
