#pragma once

#include "host_mask.h"

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct THostState
{
    enum class EState
    {
        Enabled,
        Disabled,
    };

    EState State = EState::Enabled;

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
        THostState::EState state) = 0;
    [[nodiscard]] virtual ui64 GetHostPBufferUsedSize(
        THostIndex hostIndex) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
