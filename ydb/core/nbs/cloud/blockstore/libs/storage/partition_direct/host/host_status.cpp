#include "host_status.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

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

EHostStatus THostStatusList::Get(THostIndex h) const
{
    Y_ABORT_UNLESS(h < Statuses.size());
    return Statuses[h];
}

void THostStatusList::Set(THostIndex h, EHostStatus status)
{
    Y_ABORT_UNLESS(h < Statuses.size());
    Statuses[h] = status;
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

THostMask THostStatusList::GetDisabled() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::Disabled) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

TString THostStatusList::Print() const
{
    TStringBuilder result;
    result << "[";
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (i > 0) {
            result << ",";
        }
        result << "H" << i << ":";
        switch (Statuses[i]) {
            case EHostStatus::Primary:
                result << "P";
                break;
            case EHostStatus::HandOff:
                result << "H";
                break;
            case EHostStatus::Disabled:
                result << "D";
                break;
        }
    }
    result << "]";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
