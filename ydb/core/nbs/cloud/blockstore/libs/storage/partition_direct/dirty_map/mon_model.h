#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/count_size.h>

#include <util/system/types.h>

#include <cstddef>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPBufferCounters
{
    TCountAndSize Current;
    TCountAndSize Total;
    TCountAndSize CurrentLocked;
    TCountAndSize TotalLocked;

    [[nodiscard]] TString DebugPrint() const;

    void Aggregate(const TPBufferCounters& counters);
};

struct TDirtyMapHostStats
{
    TPBufferCounters PBufferCounters;
    TCountAndSize PBuffersUsage;
    ui64 DDiskTotalBytes = 0;
    ui64 FreshTotalBytes = 0;
    ui64 RottenTotalBytes = 0;

    void Aggregate(const TDirtyMapHostStats& stats);
};

struct TDirtyMapStats
{
    size_t InflightCount = 0;
    size_t ReadyToFlushCount = 0;
    size_t ReadyToEraseCount = 0;
    size_t ReadRequestCount = 0;
    size_t ReadFromDDiskCount = 0;
    size_t ReadFromPBufferCount = 0;
    size_t CrossNodeFlushCount = 0;
    size_t InNodeFlushCount = 0;
    size_t DDiskStatesAllocatedSize = 0;
    size_t DDiskStatesUsedSize = 0;

    void Aggregate(const TDirtyMapStats& stats);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
