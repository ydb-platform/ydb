#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NClusterTracker {

using NActors::TActorId;

inline TActorId MakeClusterTrackerID() {
    static const char x[12] = "clstr_trckr";
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateClusterTracker();

} // namespace NKikimr::NPQ::NClusterTracker
