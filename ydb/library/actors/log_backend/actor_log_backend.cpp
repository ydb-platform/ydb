#include "actor_log_backend.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/logger/record.h>


TActorLogBackend::TActorLogBackend(NActors::TActorSystem* actorSystem, int logComponent)
    : ActorSystem(actorSystem)
    , LogComponent(logComponent)
{
}

void TActorLogBackend::WriteData(const TLogRecord& rec) {
    (void)rec;
}
