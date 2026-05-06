#pragma once

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
};

// An abstract interface for managing host in DirectBlockGroup
class IHostStateController
{
public:
    virtual ~IHostStateController() = default;

    virtual void SetHostState(ui8 hostIndex, THostState::EState state) = 0;
    [[nodiscard]] virtual ui64 GetHostPBufferUsedSize(ui8 hostIndex) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
