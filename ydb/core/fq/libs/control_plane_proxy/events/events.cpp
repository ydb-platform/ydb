#include "events.h"

namespace NFq {

NActors::TActorId ControlPlaneProxyActorId() {
    constexpr TStringBuf name = "YQCTLPRX";
    return NActors::TActorId(0, name);
}

} // namespace NFq
