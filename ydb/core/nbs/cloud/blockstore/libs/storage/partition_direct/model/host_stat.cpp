#include "host_stat.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void THostStat::OnError(TInstant now, EOperation operation)
{
    Y_UNUSED(operation);
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
    Y_UNUSED(operation);

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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
