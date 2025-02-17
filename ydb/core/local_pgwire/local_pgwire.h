#pragma once

#include "local_pgwire_util.h"
#include <ydb/library/actors/core/actor.h>

namespace NLocalPgWire {

inline NActors::TActorId CreateLocalPgWireProxyId(uint32_t nodeId = 0) { return NActors::TActorId(nodeId, "localpgwire"); }
NActors::IActor* CreateLocalPgWireProxy();

NActors::IActor* CreateLocalPgWireAuthActor(const TPgWireAuthData& pgWireAuthData, const NActors::TActorId& pgYdbProxy);

}
