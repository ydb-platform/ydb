#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NActors {

    class TInterconnectSessionPoolMapping {
    public:
        explicit TInterconnectSessionPoolMapping(TVector<ui32> poolIds);

        ui32 GetPoolId(ui32 peerNodeId) const;

    private:
        const TVector<ui32> PoolIds;
    };

} // namespace NActors
