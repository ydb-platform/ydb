// This file contains the fallback implementations of YTAlloc-specific stuff.
// These implementations are annotated with Y_WEAK to ensure that if the actual YTAlloc
// is available at the link time, the latter is preferred over the fallback.
#include "ytalloc.h"

#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <cstdlib>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void* Allocate(size_t size)
{
    return ::malloc(size);
}

Y_WEAK void* AllocatePageAligned(size_t size)
{
#if defined(_win_)
    return ::_aligned_malloc(size, PageSize);
#elif defined(_darwin_) || !defined(_musl_)
    return ::valloc(size);
#else
    return ::memalign(PageSize, size);
#endif
}

Y_WEAK void* AllocateSmall(size_t rank)
{
    return ::malloc(SmallRankToSize[rank]);
}

Y_WEAK void Free(void* ptr)
{
    ::free(ptr);
}

Y_WEAK void FreeNonNull(void* ptr)
{
    Y_ASSERT(ptr);
    ::free(ptr);
}

Y_WEAK size_t GetAllocationSize(const void* /*ptr*/)
{
    return 0;
}

Y_WEAK size_t GetAllocationSize(size_t size)
{
    return size;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void SetCurrentMemoryZone(EMemoryZone /*zone*/)
{ }

Y_WEAK EMemoryZone GetCurrentMemoryZone()
{
    return EMemoryZone::Normal;
}

Y_WEAK EMemoryZone GetAllocationMemoryZone(const void* /*ptr*/)
{
    return EMemoryZone::Normal;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void SetCurrentFiberId(TFiberId /*id*/)
{ }

Y_WEAK TFiberId GetCurrentFiberId()
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void EnableLogging(TLogHandler /*logHandler*/)
{ }

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void SetBacktraceProvider(TBacktraceProvider /*provider*/)
{ }

Y_WEAK void SetBacktraceFormatter(TBacktraceFormatter /*formatter*/)
{ }

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void EnableStockpile()
{ }

Y_WEAK void SetStockpileInterval(TDuration /*value*/)
{ }

Y_WEAK void SetStockpileThreadCount(int /*value*/)
{ }

Y_WEAK void SetStockpileSize(size_t /*value*/)
{ }

Y_WEAK void SetLargeUnreclaimableCoeff(double /*value*/)
{ }

Y_WEAK void SetMinLargeUnreclaimableBytes(size_t /*value*/)
{ }

Y_WEAK void SetMaxLargeUnreclaimableBytes(size_t /*value*/)
{ }

Y_WEAK void SetTimingEventThreshold(TDuration /*value*/)
{ }

Y_WEAK void SetAllocationProfilingEnabled(bool /*value*/)
{ }

Y_WEAK void SetAllocationProfilingSamplingRate(double /*rate*/)
{ }

Y_WEAK void SetSmallArenaAllocationProfilingEnabled(size_t /*rank*/, bool /*value*/)
{ }

Y_WEAK void SetLargeArenaAllocationProfilingEnabled(size_t /*rank*/, bool /*value*/)
{ }

Y_WEAK void SetProfilingBacktraceDepth(int /*depth*/)
{ }

Y_WEAK void SetMinProfilingBytesUsedToReport(size_t /*size*/)
{ }

Y_WEAK void SetEnableEagerMemoryRelease(bool /*value*/)
{ }

Y_WEAK void SetEnableMadvisePopulate(bool /*value*/)
{ }

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TEnumIndexedArray<ETotalCounter, ssize_t> GetTotalAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<ESmallCounter, ssize_t> GetSmallAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<ELargeCounter, ssize_t> GetLargeAllocationCounters()
{
    return {};
}

Y_WEAK std::array<TEnumIndexedArray<ESmallArenaCounter, ssize_t>, SmallRankCount> GetSmallArenaAllocationCounters()
{
    return {};
}

Y_WEAK std::array<TEnumIndexedArray<ELargeArenaCounter, ssize_t>, LargeRankCount> GetLargeArenaAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<EHugeCounter, ssize_t> GetHugeAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<ESystemCounter, ssize_t> GetSystemAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<EUndumpableCounter, ssize_t> GetUndumpableAllocationCounters()
{
    return {};
}

Y_WEAK TEnumIndexedArray<ETimingEventType, TTimingEventCounters> GetTimingEventCounters()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::vector<TProfiledAllocation> GetProfiledAllocationStatistics()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc

