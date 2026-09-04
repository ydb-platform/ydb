#pragma once

#include "count_size.h"
#include "host.h"

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct THostState
{
    EHostState State = EHostState::Online;

    TCountAndSize UsedPBuffers;

    // Debug purposes
    [[nodiscard]] TString DebugPrint() const;
};

// An abstract interface for managing host in DirectBlockGroup
class IHostStateController
{
public:
    virtual ~IHostStateController() = default;

    virtual void SetHostState(
        THostIndex hostIndex,
        EHostState oldState,
        EHostState newState) = 0;

    [[nodiscard]] virtual TCountAndSize GetPBuffersUsage(
        THostIndex hostIndex) const = 0;

    virtual void QueryAddHost() = 0;

    virtual void QueryRemoveHost(THostIndex hostIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
