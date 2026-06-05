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
    // For PBuffer, Primary host should be preferred for writing a new data.
    // For DDisk, data must be flushed to Primary host.
    Primary = 0,

    // For PBuffer, HandOff should be used if data could not be written to the
    // primary host. This role is not used for DDisk.
    HandOff = 1,

    // The host should not be used.
    None = 2,
};

// Determines the current status of the host, whether it can be used or not.
enum class EHostState
{
    // All OK.
    Online,

    // Errors have been noticed on the host, but they are not fatal and last for
    // a short time. There is hope that everything will work fine, the host is
    // not used for writing and reading, but some requests are still hedged
    // against it to see when it will work.
    TemporaryOffline,

    // Fatal errors have been noticed on the host, or they have been going on
    // for a long time. There is no hope that everything will work normally
    // anymore. Reconfiguration is underway to avoid using this host.
    Offline,
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
