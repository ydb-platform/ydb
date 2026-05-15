#include "host_roles.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

THostRoles::THostRoles(size_t hostCount)
    : Count(hostCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);

    Assignments.fill(EHostRole::None);
}

// static
THostRoles THostRoles::MakeRotating(
    size_t hostCount,
    ui32 vChunkIndex,
    size_t primaryCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Y_ABORT_UNLESS(primaryCount <= hostCount);

    THostRoles result(hostCount);
    for (size_t i = 0; i < hostCount; ++i) {
        result.Assignments[i] = EHostRole::HandOff;
    }
    for (size_t i = 0; i < primaryCount; ++i) {
        const size_t idx = (i + vChunkIndex) % hostCount;
        result.Assignments[idx] = EHostRole::Primary;
    }
    return result;
}

size_t THostRoles::HostCount() const
{
    return Count;
}

EHostRole THostRoles::GetRole(THostIndex host) const
{
    Y_ABORT_UNLESS(host < Count);

    return Assignments[host];
}

void THostRoles::SetRole(THostIndex host, EHostRole assignment)
{
    Y_ABORT_UNLESS(host < Count);

    Assignments[host] = assignment;
}

THostMask THostRoles::GetPrimary() const
{
    THostMask result;
    for (size_t i = 0; i < Count; ++i) {
        if (Assignments[i] == EHostRole::Primary) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostRoles::GetHandOff() const
{
    THostMask result;
    for (size_t i = 0; i < Count; ++i) {
        if (Assignments[i] == EHostRole::HandOff) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostRoles::GetActive() const
{
    return GetPrimary().Include(GetHandOff());
}

TString THostRoles::DebugPrint() const
{
    TStringBuilder result;
    for (size_t i = 0; i < Count; ++i) {
        if (i) {
            result << ";";
        }
        result << ToString(Assignments[i]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
