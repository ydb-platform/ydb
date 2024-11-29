#include "data_plane.h"

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId() {
    constexpr TStringBuf name = "ROW_DISP_DP";
    return NActors::TActorId(0, name);
}

NActors::TActorId PurecalcCompileServiceActorId() {
    constexpr TStringBuf name = "PC_COMPL_DP";
    return NActors::TActorId(0, name);
}

} // namespace NFq
