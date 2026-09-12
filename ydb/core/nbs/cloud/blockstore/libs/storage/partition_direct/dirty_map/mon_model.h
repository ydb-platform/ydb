#pragma once

#include <util/system/types.h>

#include <cstddef>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TDirtyMapStats
{
    size_t InflightCount = 0;
    size_t ReadyToFlushCount = 0;
    size_t ReadyToEraseCount = 0;

    void Aggregate(const TDirtyMapStats& stats);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
