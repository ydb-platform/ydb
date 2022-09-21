#pragma once

#include <ydb/core/base/events.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/defs.h>
#include <library/cpp/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NPQ::NClusterDiscovery {
    using NActors::TActorId;

    inline TActorId MakeClusterDiscoveryServiceID() {
        const char x[TActorId::MaxServiceIDLength] = "pq_discosvc";
        return TActorId(0, TStringBuf(x, TActorId::MaxServiceIDLength));
    }

    NActors::IActor* CreateClusterDiscoveryService(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

} // namespace NKikimr::NPQ::NClusterDiscovery
