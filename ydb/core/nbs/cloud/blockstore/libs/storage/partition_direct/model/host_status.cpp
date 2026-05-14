#include "host_status.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

THostStatusList::THostStatusList(size_t hostCount)
    : Count(hostCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Statuses.fill(EHostStatus::Disabled);
}

// static
THostStatusList THostStatusList::MakeRotating(
    size_t hostCount,
    ui32 vChunkIndex,
    size_t primaryCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Y_ABORT_UNLESS(primaryCount <= hostCount);

    THostStatusList result(hostCount);
    for (size_t i = 0; i < hostCount; ++i) {
        result.Statuses[i] = EHostStatus::HandOff;
    }
    for (size_t i = 0; i < primaryCount; ++i) {
        const size_t idx = (i + vChunkIndex) % hostCount;
        result.Statuses[idx] = EHostStatus::Primary;
    }
    return result;
}

size_t THostStatusList::HostCount() const
{
    return Count;
}

EHostStatus THostStatusList::Get(THostIndex host) const
{
    Y_ABORT_UNLESS(host < Count);
    return Statuses[host];
}

void THostStatusList::Set(THostIndex host, EHostStatus status)
{
    Y_ABORT_UNLESS(host < Count);
    Statuses[host] = status;
}

THostMask THostStatusList::GetPrimary() const
{
    THostMask result;
    for (size_t i = 0; i < Count; ++i) {
        if (Statuses[i] == EHostStatus::Primary) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostStatusList::GetHandOff() const
{
    THostMask result;
    for (size_t i = 0; i < Count; ++i) {
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

TString THostStatusList::DebugPrint() const
{
    TStringBuilder result;
    for (size_t i = 0; i < Count; ++i) {
        if (i) {
            result << ";";
        }
        result << ToString(Statuses[i]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
