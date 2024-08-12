#include "slab_allocator.h"

#include "memory_usage_tracker.h"

#include <yt/yt/core/misc/atomic_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/malloc/malloc.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

namespace {

// Maps small chunk ranks to size in bytes.
constexpr auto SmallRankToSize = std::to_array<size_t>({
    0,
    16, 32, 48, 64, 96, 128,
    192, 256, 384, 512, 768, 1024, 1536, 2048,
    3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768
});

// Helper array for mapping size to small chunk rank.
constexpr auto SizeToSmallRank1 = std::to_array<ui64>({
    1, 1, 1, 2, 2, // 16, 32
    3, 3, 4, 4, // 48, 64
    5, 5, 5, 5, 6, 6, 6, 6, // 96, 128
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, // 192, 256
    9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, // 384
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, // 512
});

// Helper array for mapping size to small chunk rank.
constexpr auto SizeToSmallRank2 = std::to_array<ui8>({
    10, 10, 11, 12, // 512, 512, 768, 1022
    13, 13, 14, 14, // 1536, 2048
    15, 15, 15, 15, 16, 16, 16, 16, // 3072, 4096
    17, 17, 17, 17, 17, 17, 17, 17, 18, 18, 18, 18, 18, 18, 18, 18, // 6144, 8192
    19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, // 12288
    20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, // 16384
    21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21,
    21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, // 22576
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22,
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, // 32768
});

constexpr size_t LargeAllocationSizeThreshold = 32_KB;

constexpr size_t SizeToSmallRank(size_t size)
{
    if (size <= 512) {
        return SizeToSmallRank1[(size + 7) >> 3];
    } else {
        if (size <= LargeAllocationSizeThreshold) {
            return SizeToSmallRank2[(size - 1) >> 8];
        } else {
            return 0;
        }
    }
}

} // namespace

/////////////////////////////////////////////////////////////////////////////

struct TArenaCounters
{
    TArenaCounters() = default;

    explicit TArenaCounters(const NProfiling::TProfiler& profiler)
        : AllocatedItems(profiler.Counter("/lookup/allocated_items"))
        , FreedItems(profiler.Counter("/lookup/freed_items"))
        , AliveItems(profiler.Gauge("/lookup/alive_items"))
        , ArenaSize(profiler.Gauge("/lookup/arena_size"))
    { }

    NProfiling::TCounter AllocatedItems;
    NProfiling::TCounter FreedItems;
    NProfiling::TGauge AliveItems;
    NProfiling::TGauge ArenaSize;
};

/////////////////////////////////////////////////////////////////////////////

class TSmallArena final
    : public TRefTracked<TSmallArena>
    , public TArenaCounters
{
public:
    static constexpr bool EnableHazard = true;

    struct TFreeListItem
        : public TFreeListItemBase<TFreeListItem>
    { };

    using TSimpleFreeList = TFreeList<TFreeListItem>;

    TSmallArena(
        size_t rank,
        size_t segmentSize,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler)
        : TArenaCounters(profiler.WithTag("rank", ToString(rank)))
        , ObjectSize_(SmallRankToSize[rank])
        , ObjectCount_(segmentSize / ObjectSize_)
        , MemoryTracker_(std::move(memoryTracker))
    {
        YT_VERIFY(ObjectCount_ > 0);
    }

    void* Allocate()
    {
        auto* obj = FreeList_.Extract();
        if (Y_LIKELY(obj)) {
            AllocatedItems.Increment();
            AliveItems.Update(GetRefCounter(this)->GetRefCount() + 1);
            // Fast path.
            return obj;
        }

        return AllocateSlow();
    }

    void Free(void* obj)
    {
        FreedItems.Increment();
        AliveItems.Update(GetRefCounter(this)->GetRefCount() - 1);
        FreeList_.Put(static_cast<TFreeListItem*>(obj));
        Unref(this);
    }

    ~TSmallArena()
    {
        constexpr auto& Logger = LockFreeLogger;

        FreeList_.ExtractAll();

        size_t segmentCount = 0;
        auto* segment = Segments_.ExtractAll();
        while (segment) {
            auto* next = segment->Next.load(std::memory_order::acquire);
            ::free(segment);
            segment = next;
            ++segmentCount;
        }

        YT_VERIFY(static_cast<ssize_t>(segmentCount) == SegmentCount_.load());

        size_t totalSize = segmentCount * (sizeof(TFreeListItem) + ObjectSize_ * ObjectCount_);

        YT_LOG_TRACE("Destroying arena (ObjectSize: %v, TotalSize: %v)",
            ObjectSize_,
            totalSize);

        if (MemoryTracker_) {
            MemoryTracker_->Release(totalSize);
        }

    #ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::FreeSpace(GetRefCountedTypeCookie<TSmallArena>(), totalSize);
    #endif
    }

    bool IsReallocationNeeded() const
    {
        auto refCount = GetRefCounter(this)->GetRefCount();
        auto segmentCount = SegmentCount_.load();

        auto maxRefCount = static_cast<ssize_t>(segmentCount * ObjectCount_) + 4;
        return segmentCount > 1 && refCount * 2 < maxRefCount || segmentCount == 1 && refCount == 2;
    }

    IMemoryUsageTrackerPtr GetMemoryTracker() const
    {
        return MemoryTracker_;
    }

private:
    const size_t ObjectSize_;
    const size_t ObjectCount_;

    TSimpleFreeList FreeList_;
    TSimpleFreeList Segments_;
    std::atomic<int> SegmentCount_ = 0;
    const IMemoryUsageTrackerPtr MemoryTracker_;

    std::pair<TFreeListItem*, TFreeListItem*> BuildFreeList(char* ptr)
    {
        auto head = reinterpret_cast<TFreeListItem*>(ptr);

        // Build chain of chunks.
        auto objectCount = ObjectCount_;
        auto objectSize = ObjectSize_;

        YT_VERIFY(objectCount > 0);
        YT_VERIFY(objectSize > 0);
        auto lastPtr = ptr + objectSize * (objectCount - 1);

        while (objectCount-- > 1) {
            auto* current = reinterpret_cast<TFreeListItem*>(ptr);
            ptr += objectSize;

            current->Next.store(reinterpret_cast<TFreeListItem*>(ptr), std::memory_order::release);
        }

        YT_VERIFY(ptr == lastPtr);

        auto* current = reinterpret_cast<TFreeListItem*>(ptr);
        current->Next.store(nullptr, std::memory_order::release);

        return {head, current};
    }

    void* AllocateSlow()
    {
        // For large chunks it is better to allocate SegmentSize + sizeof(TFreeListItem) space
        // than allocate SegmentSize and use ObjectCount_ - 1.
        auto totalSize = sizeof(TFreeListItem) + ObjectSize_ * ObjectCount_;

        if (MemoryTracker_ && !MemoryTracker_->TryAcquire(totalSize).IsOK()) {
            return nullptr;
        }

        auto segmentCount = SegmentCount_.load();
        auto refCount = GetRefCounter(this)->GetRefCount();
        constexpr auto& Logger = LockFreeLogger;

        YT_LOG_TRACE("Allocating segment (ObjectSize: %v, RefCount: %v, SegmentCount: %v, TotalObjectCapacity: %v, TotalSize: %v)",
            ObjectSize_,
            refCount,
            segmentCount,
            segmentCount * ObjectCount_,
            segmentCount * totalSize);

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::AllocateSpace(GetRefCountedTypeCookie<TSmallArena>(), totalSize);
#endif

        auto* ptr = ::malloc(totalSize);

        // Save segments in list to free them in destructor.
        Segments_.Put(static_cast<TFreeListItem*>(ptr));

        ++SegmentCount_;

        AllocatedItems.Increment();
        AliveItems.Update(refCount + 1);
        ArenaSize.Update((segmentCount + 1) * totalSize);

        auto [head, tail] = BuildFreeList(static_cast<char*>(ptr) + sizeof(TFreeListItem));

        // Extract one element.
        auto* next = head->Next.load();
        FreeList_.Put(next, tail);
        return head;
    }
};

DEFINE_REFCOUNTED_TYPE(TSmallArena);

/////////////////////////////////////////////////////////////////////////////

class TLargeArena
    : public TRefTracked<TLargeArena>
    , public TArenaCounters
{
public:
    TLargeArena(IMemoryUsageTrackerPtr memoryTracker, const NProfiling::TProfiler& profiler)
        : TArenaCounters(profiler.WithTag("rank", "large"))
        , MemoryTracker_(std::move(memoryTracker))
    { }

    void* Allocate(size_t size)
    {
        auto allocatedSize = size + sizeof(TSizeHeader);
        if (!TryAcquireMemory(allocatedSize)) {
            return nullptr;
        }

        auto itemCount = ++RefCount_;
        auto ptr = malloc(allocatedSize);

        auto header = reinterpret_cast<TSizeHeader*>(ptr);
        header->Size = allocatedSize;

        AllocatedItems.Increment();
        AliveItems.Update(itemCount);

        return reinterpret_cast<void*>(reinterpret_cast<char*>(ptr) + sizeof(TSizeHeader));
    }

    void Free(void* ptr)
    {
        ptr = reinterpret_cast<void*>(reinterpret_cast<char*>(ptr) - sizeof(TSizeHeader));

        auto allocatedSize = reinterpret_cast<TSizeHeader*>(ptr)->Size;
        ReleaseMemory(allocatedSize);
        free(ptr);
        FreedItems.Increment();
        AliveItems.Update(RefCount_.load() - 1);
        Unref();
    }

    size_t Unref()
    {
        auto count = --RefCount_;
        if (count == 0) {
            delete this;
        }
        return count;
    }

    bool TryAcquireMemory(size_t size)
    {
        if (!MemoryTracker_) {
            return true;
        }

        auto overheadMemory = OverheadMemory_.load();
        do {
            if (overheadMemory < size) {
                auto targetAcquire = std::max(TSlabAllocator::AcquireMemoryGranularity, size);
                auto result = MemoryTracker_->TryAcquire(targetAcquire);
                if (result.IsOK()) {
                    OverheadMemory_.fetch_add(targetAcquire - size);
                    auto arenaSize = AcquiredMemory_.fetch_add(targetAcquire) + targetAcquire;
                    ArenaSize.Update(arenaSize);
                    return true;
                } else {
                    return false;
                }
            }
        } while (!OverheadMemory_.compare_exchange_weak(overheadMemory, overheadMemory - size));

        return true;
    }

    void ReleaseMemory(size_t size)
    {
        if (!MemoryTracker_) {
            return;
        }

        auto overheadMemory = OverheadMemory_.load();

        while (overheadMemory + size > TSlabAllocator::AcquireMemoryGranularity) {
            auto halfMemoryGranularity = TSlabAllocator::AcquireMemoryGranularity / 2;
            if (OverheadMemory_.compare_exchange_weak(overheadMemory, halfMemoryGranularity)) {
                auto releasedMemory = overheadMemory + size - halfMemoryGranularity;
                MemoryTracker_->Release(releasedMemory);
                auto arenaSize = AcquiredMemory_.fetch_sub(releasedMemory) - releasedMemory;
                ArenaSize.Update(arenaSize);
                return;
            }
        }

        OverheadMemory_.fetch_add(size);
    }

private:
    struct TSizeHeader
    {
        i64 Size;
        i64 Dummy;
    };

    const IMemoryUsageTrackerPtr MemoryTracker_;

    // One ref from allocator plus refs from allocated objects.
    std::atomic<int> RefCount_ = 1;

    std::atomic<size_t> OverheadMemory_ = 0;
    std::atomic<size_t> AcquiredMemory_ = 0;
};

/////////////////////////////////////////////////////////////////////////////

namespace {

TLargeArena* TryGetLargeArenaFromTag(uintptr_t tag)
{
    return tag & 1ULL ? reinterpret_cast<TLargeArena*>(tag & ~1ULL) : nullptr;
}

TSmallArena* GetSmallArenaFromTag(uintptr_t tag)
{
    return reinterpret_cast<TSmallArena*>(tag);
}

uintptr_t MakeTagFromArena(TLargeArena* arena)
{

    auto result = reinterpret_cast<uintptr_t>(arena);
    YT_ASSERT((result & 1ULL) == 0);
    return result | 1ULL;
}

uintptr_t MakeTagFromArena(TSmallArena* segment)
{
    auto result = reinterpret_cast<uintptr_t>(segment);
    YT_ASSERT((result & 1ULL) == 0);
    return result & ~1ULL;
}

const uintptr_t* GetHeaderFromPtr(const void* ptr)
{
    return static_cast<const uintptr_t*>(ptr) - 1;
}

uintptr_t* GetHeaderFromPtr(void* ptr)
{
    return static_cast<uintptr_t*>(ptr) - 1;
}

} // namespace

/////////////////////////////////////////////////////////////////////////////

TSlabAllocator::TSlabAllocator(
    const NProfiling::TProfiler& profiler,
    IMemoryUsageTrackerPtr memoryTracker)
    : Profiler_(profiler)
{
    static_assert(SmallRankCount == SmallRankToSize.size(), "Wrong SmallRankCount");
    static_assert(SegmentSize >= LargeAllocationSizeThreshold, "Segment size violation");
    static_assert(AcquireMemoryGranularity % 2 == 0, "AcquireMemoryGranularity must be divisible by 2");

    for (size_t rank = 1; rank < SmallRankCount; ++rank) {
        // There is no std::make_unique overload with custom deleter.
        SmallArenas_[rank].Store(New<TSmallArena>(rank, TSlabAllocator::SegmentSize, memoryTracker, Profiler_));
    }

    LargeArena_.reset(new TLargeArena(memoryTracker, profiler));
}

void TSlabAllocator::TLargeArenaDeleter::operator() (TLargeArena* arena)
{
    arena->Unref();
}

void* TSlabAllocator::Allocate(size_t size)
{
    size += sizeof(uintptr_t);

    uintptr_t tag = 0;
    void* ptr = nullptr;
    if (size < LargeAllocationSizeThreshold) {
        auto rank = SizeToSmallRank(size);

        auto arena = SmallArenas_[rank].Acquire();
        YT_VERIFY(arena);
        ptr = arena->Allocate();
        if (ptr) {
            auto* arenaPtr = arena.Release();
            tag = MakeTagFromArena(arenaPtr);
        }
    } else {
        ptr = LargeArena_->Allocate(size);
        tag = MakeTagFromArena(LargeArena_.get());
    }

    if (!ptr) {
        return nullptr;
    }

    // Mutes TSAN data race with write Next in TFreeList::Push.
    auto* header = static_cast<std::atomic<uintptr_t>*>(ptr);
    header->store(tag, std::memory_order::release);

    return header + 1;
}

bool TSlabAllocator::IsReallocationNeeded() const
{
    for (size_t rank = 1; rank < SmallRankCount; ++rank) {
        auto arena = SmallArenas_[rank].Acquire();
        if (arena->IsReallocationNeeded()) {
            return true;
        }
    }
    return false;
}

bool TSlabAllocator::IsReallocationNeeded(const void* ptr)
{
    auto tag = *GetHeaderFromPtr(ptr);
    return !TryGetLargeArenaFromTag(tag) && GetSmallArenaFromTag(tag)->IsReallocationNeeded();
}

bool TSlabAllocator::ReallocateArenasIfNeeded()
{
    bool hasReallocatedArenas = false;
    for (size_t rank = 1; rank < SmallRankCount; ++rank) {
        auto arena = SmallArenas_[rank].Acquire();
        if (arena->IsReallocationNeeded()) {
            SmallArenas_[rank].SwapIfCompare(
                arena,
                New<TSmallArena>(rank, TSlabAllocator::SegmentSize, arena->GetMemoryTracker(), Profiler_));
            hasReallocatedArenas = true;
        }
    }
    return hasReallocatedArenas;
}

void TSlabAllocator::Free(void* ptr)
{
    YT_ASSERT(ptr);
    auto* header = GetHeaderFromPtr(ptr);
    auto tag = *header;

    if (auto* largeArena = TryGetLargeArenaFromTag(tag)) {
        largeArena->Free(header);
    } else {
        auto* arenaPtr = GetSmallArenaFromTag(tag);
        arenaPtr->Free(header);
    }
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

