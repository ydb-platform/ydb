#include "public.h"

#include <yt/yt/core/ypath/token.h>

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

std::string GetTabletCellBundlePath(const std::string& name)
{
    return "//sys/tablet_cell_bundles/" + NYPath::ToYPathLiteral(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

