#include "data_plane.h"

namespace NFq {

NActors::TActorId YqQuoterServiceActorId() {
    constexpr TStringBuf name = "RATE_LIM_DP";
    return NActors::TActorId(0, name);
}

} // namespace NFq
