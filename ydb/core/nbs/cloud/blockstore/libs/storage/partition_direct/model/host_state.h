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

<<<<<<< HEAD
    [[nodiscard]] virtual ui64 GetHostPBufferUsedSize(
=======
    virtual void PersistHostHealth(
        THostIndex hostIndex,
        EHostHealth oldHealth,
        EHostHealth newHealth) = 0;

    [[nodiscard]] virtual TCountAndSize GetPBuffersUsage(
>>>>>>> 8a60344d593 (NBS-7513:  Add Oracle->PartitionDirect PersistHostHealth path (#52610))
        THostIndex hostIndex) const = 0;

    virtual void QueryAddHost() = 0;

    virtual void QueryRemoveHost(THostIndex hostIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
