#pragma once

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Disk-wide state of one partition. TFastPathService owns the state;
// each DirectBlockGroup's oracle reads it when choosing the write mode.
class IDiskStateProvider
{
public:
    virtual ~IDiskStateProvider() = default;

    // Number of vchunk writes in flight across the whole disk.
    [[nodiscard]] virtual size_t GetInflightWriteCount() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
