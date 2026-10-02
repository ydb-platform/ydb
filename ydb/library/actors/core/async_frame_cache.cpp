#include "async_frame_cache.h"

#include "thread_context.h"

namespace NActors {

TAsyncFrameCache::TAsyncFrameCache(size_t sizeBytes) noexcept
    : SizeBytes(sizeBytes)
{
}

TAsyncFrameCache* TAsyncFrameCache::GetCurrent() noexcept {
    if (!TlsThreadContext) {
        return nullptr;
    }
    return TlsThreadContext->AsyncFrameCache;
}

TScopedAsyncFrameCache::TScopedAsyncFrameCache(TAsyncFrameCache& cache)
    : PreviousContext(TlsThreadContext)
    , PreviousCache(nullptr)
    , OwnedContext(nullptr)
{
    if (PreviousContext) {
        PreviousCache = PreviousContext->AsyncFrameCache;
        PreviousContext->AsyncFrameCache = &cache;
    } else {
        OwnedContext = new TThreadContext(0, nullptr, nullptr);
        OwnedContext->AsyncFrameCache = &cache;
        TlsThreadContext = OwnedContext;
    }
}

TScopedAsyncFrameCache::~TScopedAsyncFrameCache() {
    if (PreviousContext) {
        PreviousContext->AsyncFrameCache = PreviousCache;
    } else {
        TlsThreadContext = nullptr;
        delete OwnedContext;
    }
}

TAsyncFrameCache::~TAsyncFrameCache() {
    for (size_t index = 0; index < BinCount; ++index) {
        auto& head = Bins[index];
        while (auto* frame = head) {
            UnpoisonMemory(frame, sizeof(TIdleFrameLink));
            head = frame->Next;
            DeleteFrame(frame, BinCapacity(index));
        }
    }
}

TAsyncFrameCache::TStats TAsyncFrameCache::GetStats() const noexcept {
    TStats stats;
    stats.CachedBytes = CachedBytes;
    stats.HeapAllocations = HeapAllocations;
    for (const auto& count : Counts) {
        if (const auto frames = count.load(std::memory_order_relaxed)) {
            ++stats.SizeClasses;
            stats.CachedFrames += frames;
        }
    }
    return stats;
}

TAsyncFrameCache::TProcessStats TAsyncFrameCache::GetCachedStats() const noexcept {
    TProcessStats stats;
    for (size_t index = 0; index < BinCount; ++index) {
        const auto frames = Counts[index].load(std::memory_order_relaxed);
        stats.CachedFrames += frames;
        stats.CachedBytes += frames * BinCapacity(index);
    }
    return stats;
}

} // namespace NActors
