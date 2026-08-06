#include "interconnect_session_pool_mapping.h"

#include <util/generic/yexception.h>

namespace NActors {

    TInterconnectSessionPoolMapping::TInterconnectSessionPoolMapping(TVector<ui32> poolIds)
        : PoolIds(std::move(poolIds))
    {
        Y_ENSURE(!PoolIds.empty(), "Interconnect session pool mapping must not be empty");
    }

    ui32 TInterconnectSessionPoolMapping::GetPoolId(ui32 peerNodeId) const {
        return PoolIds[peerNodeId % PoolIds.size()];
    }

} // namespace NActors
