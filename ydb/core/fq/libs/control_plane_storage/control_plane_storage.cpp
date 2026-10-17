#include "control_plane_storage.h"

namespace NFq {

NActors::TActorId ControlPlaneStorageServiceActorId(ui32 nodeId) {
    constexpr TStringBuf name = "CTRLSTORAGE";
    return NActors::TActorId(nodeId, name);
}

} // namespace NFq
