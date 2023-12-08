#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ {

inline NActors::TActorId MakePQDReadCacheServiceActorId() {
    return NActors::TActorId(0, "PQCacheProxy");
}

IActor* CreatePQDReadCacheService(const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace
