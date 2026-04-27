#include "host_stat.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void THostStat::OnRequest(EOperation operation)
{
    ++InflightByOperation[static_cast<size_t>(operation)];
}

void THostStat::OnError(TInstant now, EOperation operation)
{
    auto& inflight = InflightByOperation[static_cast<size_t>(operation)];
    // Clamp to 0 to be defensive against unbalanced OnRequest/OnError pairs.
    if (inflight > 0) {
        --inflight;
    }

    if (!LastError) {
        LastError = now;
    }
    ++ErrorCount;
}

void THostStat::OnSuccess(
    TInstant now,
    TDuration executionTime,
    EOperation operation)
{
    Y_UNUSED(executionTime);

    auto& inflight = InflightByOperation[static_cast<size_t>(operation)];
    if (inflight > 0) {
        --inflight;
    }

    LastSuccess = now;
    LastError = TInstant();
    ErrorCount = 0;
}

TDuration THostStat::ErrorsDuration(TInstant now, size_t* errorCount) const
{
    if (errorCount) {
        *errorCount = ErrorCount;
    }
    if (LastError) {
        return now - LastError;
    }
    return TDuration();
}

size_t THostStat::InflightCount(EOperation operation) const
{
    return InflightByOperation[static_cast<size_t>(operation)];
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
