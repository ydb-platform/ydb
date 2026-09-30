#pragma once

#include <util/generic/bitops.h>
#include <util/generic/noncopyable.h>
#include <util/generic/size_literals.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <new>

#if defined(_asan_enabled_)
#include <sanitizer/asan_interface.h>
#endif
#if defined(_msan_enabled_)
#include <sanitizer/msan_interface.h>
#endif

namespace NActors {

// Worker-local idle (free) allocations only. Frames in use are owned by user, not this cache.
// Preserves default new alignment (__STDCPP_DEFAULT_NEW_ALIGNMENT__, usually 16);
// extended frame alignment is unsupported.
class TAsyncFrameCache : TNonCopyable {
public:
    static constexpr size_t DefaultSizeBytes = 4_MB;

    // cache buckets are 1 KiB, 2 KiB, 4 KiB, ..., 64 KiB
    static constexpr size_t MinCachedFrameSize = 1_KB;
    static constexpr size_t MinCachedFrameSizeLog2 = MostSignificantBitCT(MinCachedFrameSize);
    static constexpr size_t MaxCachedFrameSize = 64_KB;
    static constexpr size_t BinCount = MostSignificantBitCT(MaxCachedFrameSize / MinCachedFrameSize) + 1;

    static_assert((MinCachedFrameSize & (MinCachedFrameSize - 1)) == 0);
    static_assert((MaxCachedFrameSize & (MaxCachedFrameSize - 1)) == 0);
    static_assert(MaxCachedFrameSize >= MinCachedFrameSize);

    struct TStats {
        size_t SizeClasses = 0;
        size_t CachedFrames = 0;
        size_t CachedBytes = 0;
        size_t HeapAllocations = 0;
    };

    // Idle frames only; safe to sample from any thread and to sum across caches.
    struct TProcessStats {
        size_t CachedFrames = 0;
        size_t CachedBytes = 0;

        void Add(const TProcessStats& other) noexcept {
            CachedFrames += other.CachedFrames;
            CachedBytes += other.CachedBytes;
        }
    };

public:
    explicit TAsyncFrameCache(size_t sizeBytes = DefaultSizeBytes) noexcept;
    ~TAsyncFrameCache();

    [[nodiscard]] void* Allocate(size_t size);
    void Release(void* frame, size_t size) noexcept;

    TStats GetStats() const noexcept;

    size_t GetSizeBytes() const noexcept {
        return SizeBytes;
    }

    // Safe from any thread. Reads only the per-bin idle frame counts, so it is
    // approximate while the owner allocates or releases and exact when the
    // owner is quiescent.
    TProcessStats GetCachedStats() const noexcept;

public:
    static TAsyncFrameCache* GetCurrent() noexcept;

    [[nodiscard]] static void* AllocateCurrent(size_t size) {
        if (auto* cache = GetCurrent()) {
            return cache->Allocate(size);
        }
        return AllocateUncached(size);
    }

    [[nodiscard]] static void* AllocateUncached(size_t size) {
        // Round even without a worker cache: another worker may later return
        // this frame to cache (might be another thread local) without
        // knowing where it was allocated.
        if (size > MaxCachedFrameSize) {
            return ::operator new(size);
        }
        const auto capacity = BinCapacity(BinIndex(size));
        auto* frame = ::operator new(capacity);
        PrepareAllocatedFrame(frame, size, capacity);
        return frame;
    }

    static void Free(void* frame, size_t size) noexcept {
        if (auto* cache = GetCurrent()) {
            cache->Release(frame, size);
        } else {
            DeleteFrame(frame, AllocationCapacity(size));
        }
    }

private:
    // not a separately allocated list node, it is allocated on the idle frame
    struct TIdleFrameLink {
        TIdleFrameLink* Next;
    };

    static size_t BinIndex(size_t size) noexcept {
        return size <= MinCachedFrameSize ? 0 : CeilLog2(size) - MinCachedFrameSizeLog2;
    }

    static size_t BinCapacity(size_t index) noexcept {
        // note, that buckets are both power of 2 and >= 1 KiB
        return MinCachedFrameSize << index;
    }

    static size_t AllocationCapacity(size_t size) noexcept {
        return size > MaxCachedFrameSize ? size : BinCapacity(BinIndex(size));
    }

    static void UnpoisonMemory(void* frame, size_t size) noexcept {
#if defined(_asan_enabled_)
        __asan_unpoison_memory_region(frame, size);
#elif defined(_msan_enabled_)
        __msan_unpoison(frame, size);
#else
        Y_UNUSED(frame, size);
#endif
    }

    static void PoisonIdleFrame(void* frame, size_t capacity) noexcept {
#if defined(_asan_enabled_)
        __asan_poison_memory_region(frame, capacity);
#elif defined(_msan_enabled_)
        __msan_poison(frame, capacity);
#else
        Y_UNUSED(frame, capacity);
#endif
    }

    static void PrepareAllocatedFrame(void* frame, size_t size, size_t capacity) noexcept {
#if defined(_asan_enabled_)
        // Start fully poisoned, then expose only the requested prefix. This
        // also handles zero/tiny requests and reuse for a smaller frame.
        __asan_poison_memory_region(frame, capacity);
        __asan_unpoison_memory_region(frame, size);
#elif defined(_msan_enabled_)
        // Reused contents must look like a fresh, uninitialized allocation.
        __msan_allocated_memory(frame, capacity);
        Y_UNUSED(size);
#else
        Y_UNUSED(frame, size, capacity);
#endif
    }

    static void DeleteFrame(void* frame, size_t capacity) noexcept {
        UnpoisonMemory(frame, capacity);
        ::operator delete(frame);
    }

private:
    std::array<TIdleFrameLink*, BinCount> Bins{};

    // The cache owner is the only writer. Use loads/stores, not atomic
    // read-modify-write operations, on the allocation and release paths.
    // Other threads may only load these counts (see GetCachedStats).
    std::array<std::atomic<size_t>, BinCount> Counts{};

    const size_t SizeBytes;
    size_t CachedBytes = 0;
    size_t HeapAllocations = 0;
};

// has to be inline, so that compiler fold it in the coroutine allocation path
// (allocation size in that case is not runtime and rather a constant)
inline void* TAsyncFrameCache::Allocate(size_t size) {
    if (size <= MaxCachedFrameSize) {
        const auto index = BinIndex(size);
        auto& head = Bins[index];
        if (head) {
            auto* frame = head;
            UnpoisonMemory(frame, sizeof(TIdleFrameLink));
            head = frame->Next;
            CachedBytes -= BinCapacity(index);
            auto& count = Counts[index];
            count.store(count.load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
            PrepareAllocatedFrame(frame, size, BinCapacity(index));
            return frame;
        }
    }
    void* frame = AllocateUncached(size);
    ++HeapAllocations;
    return frame;
}

// has to be inline, so that compiler fold it in the coroutine allocation path
// (allocation size in that case is not runtime and rather a constant)
inline void TAsyncFrameCache::Release(void* frame, size_t size) noexcept {
    if (size <= MaxCachedFrameSize) {
        const auto index = BinIndex(size);
        const auto capacity = BinCapacity(index);
        if (capacity <= SizeBytes - CachedBytes) {
            auto& head = Bins[index];
            UnpoisonMemory(frame, sizeof(TIdleFrameLink));
            head = ::new (frame) TIdleFrameLink{head};
            PoisonIdleFrame(frame, capacity);
            CachedBytes += capacity;
            auto& count = Counts[index];
            count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            return;
        }
    }
    DeleteFrame(frame, AllocationCapacity(size));
}

// Test helper. Workers publish the cache through TlsThreadContext directly.
// On a thread that already has a context, replaces AsyncFrameCache and restores
// it. Otherwise installs a context for the scope. Nested bindings restore the
// previous cache. Never share a cache between concurrently executing threads.
struct TThreadContext;

class TScopedAsyncFrameCache : TNonCopyable {
public:
    explicit TScopedAsyncFrameCache(TAsyncFrameCache& cache);
    ~TScopedAsyncFrameCache();

private:
    TThreadContext* PreviousContext;
    TAsyncFrameCache* PreviousCache;

    // Non-null only when this guard installed a context because the thread had none.
    TThreadContext* OwnedContext;
};

} // namespace NActors
