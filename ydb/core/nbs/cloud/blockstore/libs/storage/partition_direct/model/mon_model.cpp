
#include "mon_model.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// static
THostSnapshot THostSnapshot::Make(
    THostIndex index,
    const THostState& state,
    EHostHealth health,
    const THostStat& hostStat,
    TLatencyByOperation latencyByOperation,
    TInstant now)
{
    return THostSnapshot{
        .Index = index,
        .State = state.State,
        .Health = health,
        .InflightByOperation = hostStat.GetInflightByOperation(),
        .Errors = hostStat.GetErrorsInfo(now),
        .LatencyByOperation = latencyByOperation};
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
