#include <library/cpp/actors/core/actor.h>

namespace NLocalPgWire {

inline NActors::TActorId CreateLocalPgWireProxyId() { return NActors::TActorId(0, "localpgwire"); }
NActors::IActor* CreateLocalPgWireProxy();

}
