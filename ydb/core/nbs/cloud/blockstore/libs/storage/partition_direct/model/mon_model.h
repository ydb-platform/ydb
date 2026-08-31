#pragma once

#include "public.h"

#include "count_size.h"
#include "host.h"
#include "host_stat.h"
#include "host_state.h"
#include "time_predictor.h"

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

struct TChaosNodeConfig
{
    enum class EChaosMode
    {
        Disabled,
        Enabled,
        Partial,
    };

    ui32 NodeId = 0;

    EChaosMode Mode = EChaosMode::Disabled;
    double LostProbability = 0.0;
    double FailProbability = 0.0;
};

struct TChaosSnapshot
{
    TVector<TChaosNodeConfig> NodeConfigs;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
