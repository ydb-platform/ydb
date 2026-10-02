#pragma once

#include <util/generic/bitops.h>
#include <util/generic/noncopyable.h>
#include <util/generic/size_literals.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <new>
#include <memory>

#if defined(_asan_enabled_)
#include <sanitizer/asan_interface.h>
#endif
#if defined(_msan_enabled_)
#include <sanitizer/msan_interface.h>
#endif

namespace NActors {

struct TAllocationCacheStats {
    size_t SizeClasses = 0;
    size_t CachedFrames = 0;
    size_t CachedBytes = 0;
    size_t HeapAllocations = 0;
};

// Idle blocks only; safe to sample from any thread and to sum across caches.
struct TAllocationCacheProcessStats {
    size_t CachedFrames = 0;
    size_t CachedBytes = 0;

    void Add(const TAllocationCacheProcessStats& other) noexcept {
        CachedFrames += other.CachedFrames;
        CachedBytes += other.CachedBytes;
    }
};

// Read-only view of counters embedded in a cache. Registered worker caches and
// their counter views remain alive until subsystem destruction; readers must
// finish before destruction starts.
struct TAllocationCacheCounters {
    const std::atomic<size_t>* Counts = nullptr;
    size_t BinCount = 0;
    size_t MinimumSize = 0;

    TAllocationCacheProcessStats GetCachedStats() const noexcept {
        TAllocationCacheProcessStats stats;
        for (size_t index = 0; index < BinCount; ++index) {
            const auto count = Counts[index].load(std::memory_order_relaxed);
            stats.CachedFrames += count;
            stats.CachedBytes += count * (MinimumSize << index);
        }
        return stats;
    }
};

// Raw blocks only: clients construct/destroy objects and must return blocks allocated
// by this same tag, with the original requested size. Ordinary new/malloc blocks
// must never be returned here. Tags define immutable power-of-two size classes;
// budgets may vary across systems without changing block compatibility.
// Preserves default new alignment (__STDCPP_DEFAULT_NEW_ALIGNMENT__, usually 16);
// extended frame alignment is unsupported.
template<class TTag>
class TAllocationCache : TNonCopyable {
public:
    // Power-of-two buckets from the tag's minimum to maximum size.
    static constexpr size_t MinAllocationSize = TTag::MinAllocationSize;
    static constexpr size_t MinAllocationSizeLog2 = MostSignificantBitCT(MinAllocationSize);
    static constexpr size_t MaxAllocationSize = TTag::MaxAllocationSize;
    static constexpr size_t BinCount = MostSignificantBitCT(MaxAllocationSize / MinAllocationSize) + 1;

    static_assert((MinAllocationSize & (MinAllocationSize - 1)) == 0);
    static_assert((MaxAllocationSize & (MaxAllocationSize - 1)) == 0);
    static_assert(MaxAllocationSize >= MinAllocationSize);
    static_assert(MinAllocationSize >= sizeof(void*));

    using TStats = TAllocationCacheStats;
    using TProcessStats = TAllocationCacheProcessStats;

public:
    explicit TAllocationCache(size_t sizeBytes)
        : SizeBytes(sizeBytes)
    {}
    ~TAllocationCache() {
        for (size_t index = 0; index < BinCount; ++index) {
            auto& head = Bins[index];
            while (auto* frame = head) {
                UnpoisonMemory(frame, sizeof(TIdleFrameLink));
                head = frame->Next;
                DeleteFrame(frame, BinCapacity(index));
            }
            Counts[index].store(0, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] Y_FORCE_INLINE void* Allocate(size_t size);
    Y_FORCE_INLINE void Release(void* frame, size_t size) noexcept;

    // Owner-thread diagnostics; off-thread readers must use GetCachedStats.
    TStats GetStats() const noexcept {
        TStats stats;
        stats.CachedBytes = CachedBytes;
        stats.HeapAllocations = HeapAllocations;
        for (size_t index = 0; index < BinCount; ++index) {
            if (const auto frames = Counts[index].load(std::memory_order_relaxed)) {
                ++stats.SizeClasses;
                stats.CachedFrames += frames;
            }
        }
        return stats;
    }

    size_t GetSizeBytes() const noexcept {
        return SizeBytes;
    }

    // Safe from any thread. Reads only the per-bin idle frame counts, so it is
    // approximate while the owner allocates or releases and exact when the
    // owner is quiescent.
    // Borrowed view for cold registration; the caller must protect cache lifetime.
    TAllocationCacheCounters GetCountersView() const noexcept {
        return {Counts.data(), BinCount, MinAllocationSize};
    }

    TProcessStats GetCachedStats() const noexcept {
        return GetCountersView().GetCachedStats();
    }


public:
    static TAllocationCache* GetCurrent() noexcept;

    [[nodiscard]] Y_FORCE_INLINE static void* AllocateCurrent(size_t size) {
        if (auto* cache = GetCurrent()) {
            return cache->Allocate(size);
        }
        return AllocateUncached(size);
    }

    [[nodiscard]] static void* AllocateUncached(size_t size) {
        // Round even without a worker cache: another worker may later return
        // this frame to cache (might be another thread local) without
        // knowing where it was allocated.
        if (size > MaxAllocationSize) {
            return ::operator new(size);
        }
        const auto capacity = BinCapacity(BinIndex(size));
        auto* frame = ::operator new(capacity);
        PrepareAllocatedFrame(frame, size, capacity);
        return frame;
    }

    Y_FORCE_INLINE static void Free(void* frame, size_t size) noexcept {
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
        return size <= MinAllocationSize ? 0 : CeilLog2(size) - MinAllocationSizeLog2;
    }

    static size_t BinCapacity(size_t index) noexcept {
        // note, that buckets are both power of 2 and >= 1 KiB
        return MinAllocationSize << index;
    }

    static size_t AllocationCapacity(size_t size) noexcept {
        return size > MaxAllocationSize ? size : BinCapacity(BinIndex(size));
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
template<class TTag>
Y_FORCE_INLINE void* TAllocationCache<TTag>::Allocate(size_t size) {
    if (size <= MaxAllocationSize) {
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
template<class TTag>
Y_FORCE_INLINE void TAllocationCache<TTag>::Release(void* frame, size_t size) noexcept {
    if (size <= MaxAllocationSize) {
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


} // namespace NActors
