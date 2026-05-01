#include "host_status.h"

#include <util/generic/yexception.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

THostStatusList::THostStatusList(size_t hostCount)
    : Statuses(hostCount, EHostStatus::Disabled)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
}

// static
THostStatusList THostStatusList::MakeRotating(
    size_t hostCount,
    ui32 vChunkIndex,
    size_t primaryCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Y_ABORT_UNLESS(primaryCount <= hostCount);

    THostStatusList result;
    result.Statuses.assign(hostCount, EHostStatus::HandOff);
    for (size_t i = 0; i < primaryCount; ++i) {
        const size_t idx = (i + vChunkIndex) % hostCount;
        result.Statuses[idx] = EHostStatus::Primary;
    }
    return result;
}

size_t THostStatusList::HostCount() const
{
    return Statuses.size();
}

EHostStatus THostStatusList::Get(THostIndex host) const
{
    Y_ABORT_UNLESS(host < Statuses.size());
    return Statuses[host];
}

void THostStatusList::Set(THostIndex host, EHostStatus status)
{
    Y_ABORT_UNLESS(host < Statuses.size());
    Statuses[host] = status;
}

THostMask THostStatusList::GetPrimary() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::Primary) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostStatusList::GetHandOff() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::HandOff) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostStatusList::GetActive() const
{
    return GetPrimary().Include(GetHandOff());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
