#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TGarbageCollectionActor::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev) {
    ACFL_DEBUG("actor", "TEvCollectGarbageResult");
    if (ev->Get()->Status == NKikimrProto::BLOCKED) {
        auto g = PassAwayGuard();
        ACFL_WARN("event", "blocked_gc_event");
        return;
    }
    GCTask->OnGCResult(ev);
    if (GCTask->IsFinished()) {
        auto g = PassAwayGuard();
        ACFL_DEBUG("actor", "TGarbageCollectionActor")("event", "finished");
        TActorContext::AsActorContext().Send(TabletActorId, std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(GCTask));
    }
}

}
