#pragma once

#include "public.h"

#include <yt/yt/core/misc/atomic_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/free_list.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSmallArena);
class TLargeArena;

////////////////////////////////////////////////////////////////////////////////

class TSlabAllocator
{
public:
    explicit TSlabAllocator(
        const NProfiling::TProfiler& profiler = {},
        IMemoryUsageTrackerPtr memoryTracker = nullptr);

    void* Allocate(size_t size);
    static void Free(void* ptr);

    bool IsReallocationNeeded() const;
    static bool IsReallocationNeeded(const void* ptr);

    bool ReallocateArenasIfNeeded();

    static constexpr size_t SegmentSize = 64_KB;
    static constexpr size_t AcquireMemoryGranularity = 500_KB;

private:
    const NProfiling::TProfiler Profiler_;

    struct TLargeArenaDeleter
    {
        void operator() (TLargeArena* arena);
    };

    static constexpr int SmallRankCount = 23;
    std::array<TAtomicPtr<TSmallArena>, SmallRankCount> SmallArenas_;
    std::unique_ptr<TLargeArena, TLargeArenaDeleter> LargeArena_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

