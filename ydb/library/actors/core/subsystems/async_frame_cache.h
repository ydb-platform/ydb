#pragma once

#include <ydb/library/actors/core/allocation_cache.h>

namespace NActors {

// The allocation ABI is independent of each actor system's retention budget.
struct TAsyncFrameCacheTag {
    static constexpr const char* Name = "AsyncFrames";
    static constexpr size_t MinAllocationSize = 1_KB;
    static constexpr size_t MaxAllocationSize = 64_KB;
};

inline constexpr size_t DefaultAsyncFrameCacheSizeBytes = 4_MB;

template<class TTag> class TAllocationCacheFrontend;
using TAsyncFrameCacheFrontend = TAllocationCacheFrontend<TAsyncFrameCacheTag>;
using TAsyncFrameCache = TAllocationCache<TAsyncFrameCacheTag>;

} // namespace NActors
