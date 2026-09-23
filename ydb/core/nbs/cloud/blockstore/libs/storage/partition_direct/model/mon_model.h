#pragma once

#include "public.h"

#include "count_size.h"
#include "host.h"
#include "host_stat.h"
#include "host_state.h"
#include "time_predictor.h"

#include <util/generic/map.h>

#include <compare>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Indexed by EOperation.
using TLatencyByOperation = std::array<TLatencyStats, OperationCount>;

struct THostSnapshot
{
    static THostSnapshot Make(
        THostIndex index,
        const THostState& state,
        EHostHealth health,
        const THostStat& hostStat,
        TLatencyByOperation latencyByOperation,
        TInstant now);

    THostIndex Index = InvalidHostIndex;
    EHostState State = EHostState::Offline;
    EHostHealth Health = EHostHealth::Offline;
    TInflightByOperation InflightByOperation{};
    THostStat::TErrorsInfo Errors;
    TCountAndSize PBuffersUsage;
    TCountAndSize AheadBlocks;
    TCountAndSize BehindBlocks;
    TLatencyByOperation LatencyByOperation;
};

// Configures per-node failure injection independently for each DBG.
struct TChaosConfig
{
    // Identifies one node in one DBG.
    struct TDbgAndNodeId
    {
        ui32 NodeId = 0;
        ui32 DbgIndex = 0;

        auto operator<=>(const TDbgAndNodeId&) const = default;
    };

    // Describes configured node behavior.
    struct TChaosNodeConfig
    {
        enum class EChaosMode
        {
            Disabled,   // Requests to the node are disabled.
            Enabled,    // Requests to the node are enabled.
            Partial,    // Only some requests to the node are disabled.
        };

        EChaosMode Mode = EChaosMode::Enabled;
        double LostProbability = 0.0;
        double FailProbability = 0.0;
    };

    TMap<TDbgAndNodeId, TChaosNodeConfig> NodeConfigs;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
