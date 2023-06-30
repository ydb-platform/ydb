#include "public.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

bool IsStableReplicaMode(ETableReplicaMode mode)
{
    return
        mode == ETableReplicaMode::Sync ||
        mode == ETableReplicaMode::Async;
}

bool IsStableReplicaState(ETableReplicaState state)
{
    return
        state == ETableReplicaState::Enabled ||
        state == ETableReplicaState::Disabled;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

