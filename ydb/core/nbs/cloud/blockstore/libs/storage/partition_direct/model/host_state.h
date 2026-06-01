#pragma once

#include "host.h"

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct THostState
{
    EHostState State = EHostState::Enabled;

    ui64 PBufferUsedSize = 0;

    // Debug purposes
    [[nodiscard]] TString DebugPrint() const;
};

// An abstract interface for managing host in DirectBlockGroup
class IHostStateController
{
public:
    virtual ~IHostStateController() = default;

    virtual void SetHostState(THostIndex hostIndex, EHostState state) = 0;

    [[nodiscard]] virtual ui64 GetHostPBufferUsedSize(
        THostIndex hostIndex) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
