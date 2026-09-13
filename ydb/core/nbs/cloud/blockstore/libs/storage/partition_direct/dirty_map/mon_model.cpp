#include "mon_model.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TString TPBufferCounters::DebugPrint() const
{
    TStringBuilder result;

    result << "{Current:" << Current.Print(true) << ", "
           << "Total:" << Total.Print(true) << ", "
           << "CurrentLocked:" << CurrentLocked.Print(true) << ", "
           << "TotalLocked:" << TotalLocked.Print(true) << "}";

    return result;
}

void TPBufferCounters::Aggregate(const TPBufferCounters& counters)
{
    Current += counters.Current;
    Total += counters.Total;
    CurrentLocked += counters.CurrentLocked;
    TotalLocked += counters.TotalLocked;
}

void TDirtyMapHostStats::Aggregate(const TDirtyMapHostStats& stats)
{
    PBufferCounters.Aggregate(stats.PBufferCounters);
    PBuffersUsage += stats.PBuffersUsage;
    DDiskTotalBytes += stats.DDiskTotalBytes;
    FreshTotalBytes += stats.FreshTotalBytes;
    RottenTotalBytes += stats.RottenTotalBytes;
}

void TDirtyMapStats::Aggregate(const TDirtyMapStats& stats)
{
    InflightCount += stats.InflightCount;
    ReadyToFlushCount += stats.ReadyToFlushCount;
    ReadyToEraseCount += stats.ReadyToEraseCount;
    ReadRequestCount += stats.ReadRequestCount;
    ReadFromDDiskCount += stats.ReadFromDDiskCount;
    ReadFromPBufferCount += stats.ReadFromPBufferCount;
    CrossNodeFlushCount += stats.CrossNodeFlushCount;
    InNodeFlushCount += stats.InNodeFlushCount;
    DDisksMemoryStats.Aggregate(stats.DDisksMemoryStats);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
