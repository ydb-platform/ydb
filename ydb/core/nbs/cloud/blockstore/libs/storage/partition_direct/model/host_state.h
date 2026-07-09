#pragma once

#include "host.h"

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct THostState
{
    EHostState State = EHostState::Online;

    ui64 PBufferUsedSize = 0;

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

    [[nodiscard]] virtual ui64 GetHostPBufferUsedSize(
        THostIndex hostIndex) const = 0;

    virtual void QueryAddHost(THostIndex newHostIndex) = 0;

    // Asks to durably remove the host from the group. Preconditions (host
    // disabled in every vchunk, its pbuffer drained, quorum preserved) are
    // validated by the implementation; an invalid request is dropped with a
    // log.
    virtual void QueryRemoveHost(THostIndex hostIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
