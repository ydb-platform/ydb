#include "events.h"

#include <ydb/library/actors/protos/actors.pb.h>

namespace NActors {

    TActorId ActorIdFromProto(const NActorsProto::TActorId& actorId) {
        return TActorId(actorId.GetRawX1(), actorId.GetRawX2());
    }

    void ActorIdToProto(const TActorId& src, NActorsProto::TActorId* dest) {
        Y_DEBUG_ABORT_UNLESS(dest);
        dest->SetRawX1(src.RawX1());
        dest->SetRawX2(src.RawX2());
    }

} // NActors
