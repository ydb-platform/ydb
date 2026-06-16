#pragma once

#include <util/system/types.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EWriteMode: ui32
{
    PBufferReplication,
    DirectPBuffersFilling,
};

class TStorageConfig;
using TStorageConfigPtr = std::shared_ptr<TStorageConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
