#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// The index of the host in the direct block group. Hosts can only be appended
// to the direct block group, so you can refer to the host by its index.
using THostIndex = ui8;

constexpr THostIndex InvalidHostIndex = 0xFF;
constexpr size_t MaxHostCount = 32;

////////////////////////////////////////////////////////////////////////////////

// What the host is intended for: to be a primary or a handoff.
enum class EHostRole: ui8
{
    Primary = 0,
    HandOff = 1,
    None = 2,
};

// Determines the current status of the host, whether it can be used or not.
enum class EHostState
{
    Enabled,
    Disabled,
};

////////////////////////////////////////////////////////////////////////////////

struct THostRoute
{
    THostIndex SourceHostIndex = InvalidHostIndex;
    THostIndex DestinationHostIndex = InvalidHostIndex;

    bool operator==(const THostRoute& other) const = default;
    bool operator<(const THostRoute& other) const;

    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

TString PrintHostIndex(THostIndex hostIndex);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
