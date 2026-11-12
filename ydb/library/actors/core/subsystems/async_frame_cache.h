#pragma once

#include <ydb/library/actors/core/subsystems/allocation_cache.h>

namespace NActors {

// The allocation ABI is independent of each actor system's retention budget.
struct TAsyncFrameCacheTag {
    static constexpr const char* Name = "AsyncFrames";
    static constexpr size_t MinAllocationSize = 1_KB;
    static constexpr size_t MaxAllocationSize = 64_KB;
};

class TAsyncFrameCache : public TAllocationCacheFamily<TAsyncFrameCacheTag> {
public:
    static constexpr size_t DefaultSizeBytes = 4_MB;

    explicit TAsyncFrameCache(size_t budget = DefaultSizeBytes)
        : TAllocationCacheFamily<TAsyncFrameCacheTag>(budget)
    {}
};

} // namespace NActors
