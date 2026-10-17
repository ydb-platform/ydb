#include "service_id.h"

namespace NFq {

NActors::TActorId ControlPlaneConfigActorId() {
    constexpr TStringBuf name = "FQCTLCFG";
    return NActors::TActorId(0, name);
}

} // namespace NFq
