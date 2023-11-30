#include <ydb/library/actors/core/actor.h>

namespace NLocalPgWire {

inline NActors::TActorId CreateLocalPgWireProxyId(uint32_t nodeId = 0) { return NActors::TActorId(nodeId, "localpgwire"); }
NActors::IActor* CreateLocalPgWireProxy();

}
