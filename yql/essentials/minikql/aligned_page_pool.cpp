#include "aligned_page_pool.h"

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/error.h>
#include <util/system/info.h>
#include <util/thread/lfstack.h>

#include <yql/essentials/public/udf/sanitizer_utils.h>
#include <yql/essentials/utils/exception_utils.h>

#include <algorithm>
#include <map>
#include <mutex>

#if defined(_win_)
    #include <util/system/winint.h>
#elif defined(_unix_)
    #include <sys/types.h>
    #include <sys/mman.h>
#endif

namespace NKikimr {

#if defined(ALLOW_DEFAULT_ALLOCATOR)
    #if defined(PROFILE_MEMORY_ALLOCATIONS)
static bool IsDefaultAllocator = true;
    #else
static bool IsDefaultAllocator = false;
    #endif
void UseDefaultAllocator() {
    // TODO: check that we didn't already used the MKQL allocator
    IsDefaultAllocator = true;
}
#endif

namespace {

bool IsDefaultArrowAllocator = false;

ui64 SYS_PAGE_SIZE = NSystemInfo::GetPageSize();

} // namespace

void UseDefaultArrowAllocator() {
    // TODO: check that we didn't already used the MKQL allocator
    IsDefaultArrowAllocator = true;
}

constexpr ui32 MidLevels = 10;
constexpr ui32 MaxMidSize = (1U << MidLevels) * TAlignedPagePool::POOL_PAGE_SIZE;
static_assert(MaxMidSize == 64 * 1024 * 1024, "Upper memory block 64 Mb");

namespace {

ui64 GetMaxMemoryMaps() {
    ui64 maxMapCount = 0;
#if defined(_unix_) && !defined(_darwin_)
    maxMapCount = FromString<ui64>(Strip(TFileInput("/proc/sys/vm/max_map_count").ReadAll()));
#endif
    return maxMapCount;
}

TString GetMemoryMapsString() {
    TStringStream ss;
    ss << " (maps: " << GetMemoryMapsCount() << " vs " << GetMaxMemoryMaps() << ")";
    return ss.Str();
}

template <typename T, bool SysAlign>
class TGlobalPools;

template <typename T, bool SysAlign>
class TGlobalPagePool {
    friend class TGlobalPools<T, SysAlign>;

public:
    TGlobalPagePool(T& provider, size_t pageSize)
        : Provider_(provider)
        , PageSize_(pageSize)
    {
    }

    ~TGlobalPagePool() {
        void* addr = nullptr;
        while (Pages_.Dequeue(&addr)) {
            FreePage(addr);
        }
    }

    void* GetPage() {
        void* page = nullptr;
        if (Pages_.Dequeue(&page)) {
            --Count_;
            NYql::NUdf::SanitizerMakeRegionInaccessible(page, PageSize_);
            return page;
        }

        return nullptr;
    }

    ui64 GetPageCount() const {
        return Count_.load(std::memory_order_relaxed);
    }

    size_t GetPageSize() const {
        return PageSize_;
    }

    size_t GetSize() const {
        return GetPageCount() * GetPageSize();
    }

private:
    size_t PushPage(void* addr) {
        if (Y_UNLIKELY(TAlignedPagePool::IsDefaultAllocatorUsed())) {
            FreePage(addr);
            return GetPageSize();
        }
        NYql::NUdf::SanitizerMakeRegionInaccessible(addr, PageSize_);
        ++Count_;
        Pages_.Enqueue(addr);
        return 0;
    }

    void FreePage(void* addr) noexcept {
        NYql::NUdf::SanitizerMakeRegionInaccessible(addr, PageSize_);
        auto res = Provider_.Munmap(addr, PageSize_);
        Y_DEBUG_ABORT_UNLESS(0 == res, "Unmap failed: %s", LastSystemErrorText());
    }

    T& Provider_;
    const size_t PageSize_;
    std::atomic<ui64> Count_ = 0;
    TLockFreeStack<void*> Pages_;
};

template <typename T, bool SysAlign>
class TGlobalPools {
public:
    static TGlobalPools<T, SysAlign>& Instance() {
        return *Singleton<TGlobalPools<T, SysAlign>>();
    }

    TGlobalPagePool<T, SysAlign>& Get(ui32 index) {
        return *Pools_[index];
    }

    const TGlobalPagePool<T, SysAlign>& Get(ui32 index) const {
        return *Pools_[index];
    }

    TGlobalPools()
        : Provider_(T::GetInstance())
    {
        Reset();
    }

    void* DoMmap(size_t size) {
        Y_DEBUG_ABORT_UNLESS(!TAlignedPagePoolImpl<T>::IsDefaultAllocatorUsed(), "No memory maps allowed while using default allocator");

        void* res = Provider_.Mmap(size);
        NYql::NUdf::SanitizerMakeRegionInaccessible(res, size);
        TotalMmappedBytes_ += size;
        return res;
    }

    void DoCleanupFreeList(ui64 targetSize) {
        for (ui32 level = 0; level <= MidLevels; ++level) {
            auto& p = Get(level);
            const size_t pageSize = p.GetPageSize();

            while (p.GetSize() >= targetSize) {
                void* page = p.GetPage();

                if (!page) {
                    break;
                }

                p.FreePage(page);
                i64 prev = TotalMmappedBytes_.fetch_sub(pageSize);
                Y_DEBUG_ABORT_UNLESS(prev >= 0);
            }
        }
    }

    void PushPage(size_t level, void* addr) {
        auto& pool = Get(level);
        size_t free = pool.PushPage(addr);
        if (Y_UNLIKELY(free > 0)) {
            i64 prev = TotalMmappedBytes_.fetch_sub(free);
            Y_DEBUG_ABORT_UNLESS(prev >= 0);
        }
    }

    void DoMunmap(void* addr, size_t size) {
        if (Y_UNLIKELY(0 != Provider_.Munmap(addr, size))) {
            TStringStream mmaps;
            const auto lastError = LastSystemError();
            if (lastError == ENOMEM) {
                mmaps << GetMemoryMapsString();
            }

            ythrow yexception() << "Munmap(0x"
                                << IntToString<16>(reinterpret_cast<uintptr_t>(addr))
                                << ", " << size << ") failed: " << LastSystemErrorText(lastError) << mmaps.Str();
        }

        i64 prev = TotalMmappedBytes_.fetch_sub(size);
        Y_DEBUG_ABORT_UNLESS(prev >= 0);
    }

    i64 GetTotalMmappedBytes() const {
        return TotalMmappedBytes_.load();
    }

    i64 GetTotalFreeListBytes() const {
        i64 bytes = 0;
        for (ui32 i = 0; i <= MidLevels; ++i) {
            bytes += Get(i).GetSize();
        }

        return bytes;
    }

    void Reset()
    {
        Pools_.clear();
        Pools_.reserve(MidLevels + 1);
        for (ui32 i = 0; i <= MidLevels; ++i) {
            Pools_.emplace_back(MakeHolder<TGlobalPagePool<T, SysAlign>>(Provider_, TAlignedPagePool::POOL_PAGE_SIZE << i));
        }
    }

private:
    T& Provider_;
    TVector<THolder<TGlobalPagePool<T, SysAlign>>> Pools_;
    std::atomic<i64> TotalMmappedBytes_{0};
};

} // namespace

#ifndef _win_
namespace {

/**
    Registry of the memory regions obtained from `mmap()`.

    Its only purpose is to tell whether a range being freed can be released with `munmap()` without
    increasing the number of the process memory mappings. Unmapping a range at the beginning or at
    the end of an owned region only shrinks (or removes) the corresponding kernel VMA, while
    unmapping a range in the middle of it splits the VMA in two - and it is exactly that splitting
    which used to exhaust `vm.max_map_count` when the page pools cleaned up their free lists.

    So the ranges that can't be unmapped for free are released with `madvise(MADV_DONTNEED)` instead:
    the resident memory is returned to the system right away, but the address space is retained until
    the neighbouring ranges are freed too and the hole becomes adjacent to a region boundary. That
    way the number of mappings never grows because of the clean-up, and yet the address space (along
    with the page tables backing it, see `VmPTE` in `/proc/self/status`) is eventually returned to
    the system instead of being leaked.

    Every `mmap()` is tracked as a separate region even when it happens to be allocated right next to
    another one (in which case the kernel merges the two mappings into a single VMA). That keeps the
    address space accounting simple, and the price is bounded: a boundary unmap that turns out to be
    interior to a merged VMA splits it, but the resulting number of mappings still stays proportional
    to the number of the regions that are alive - never to the number of the pages carved out of them.
 */
class TMappedRegions {
public:
    void Register(void* addr, size_t size) {
        const uintptr_t begin = reinterpret_cast<uintptr_t>(addr);
        const uintptr_t end = begin + AlignUp<size_t>(size, SYS_PAGE_SIZE);

        std::lock_guard guard(Lock_);

        Y_DEBUG_ABORT_UNLESS(!Regions_.contains(begin), "Region is already registered");
        Regions_[begin].End = end;
        TotalSize_ += end - begin;
    }

    size_t GetCount() const {
        std::lock_guard guard(Lock_);
        return Regions_.size();
    }

    size_t GetTotalSize() const {
        std::lock_guard guard(Lock_);
        return TotalSize_;
    }

    int Release(void* addr, size_t size) noexcept {
        uintptr_t begin = reinterpret_cast<uintptr_t>(addr);
        uintptr_t end = begin + size;

        std::lock_guard guard(Lock_);

        const auto region = FindRegion(begin, end);
        if (Y_UNLIKELY(region == Regions_.end())) {
            // Not a memory we have mapped - drop the resident pages only, since we know nothing
            // about the mapping this range belongs to.
            return DontNeed(begin, end - begin);
        }

        auto& holes = region->second.Holes;

        // Coalesce the range with the holes released earlier - together they may reach a boundary.
        auto hole = holes.upper_bound(begin);
        if (hole != holes.begin()) {
            const auto before = std::prev(hole);
            if (before->second >= begin) {
                begin = before->first;
                end = std::max(end, before->second);
                holes.erase(before);
            }
        }
        while (hole != holes.end() && hole->first <= end) {
            end = std::max(end, hole->second);
            hole = holes.erase(hole);
        }

        const bool atBegin = begin == region->first;
        const bool atEnd = end == region->second.End;

        if (!atBegin && !atEnd) {
            // Unmapping the hole would split the mapping in two, so keep the address space reserved.
            holes.emplace(begin, end);
            return DontNeed(begin, end - begin);
        }

        if (Y_UNLIKELY(0 != ::munmap(reinterpret_cast<void*>(begin), end - begin))) {
            // The range is still mapped - remember it, so the next neighbour to be freed retries.
            holes.emplace(begin, end);
            return -1;
        }

        TotalSize_ -= end - begin;

        if (atBegin && atEnd) {
            Regions_.erase(region);
        } else if (atBegin) {
            auto node = Regions_.extract(region);
            node.key() = end;
            Regions_.insert(std::move(node));
        } else {
            region->second.End = begin;
        }

        return 0;
    }

private:
    struct TRegion {
        uintptr_t End = 0;
        // Ranges that are released, but still mapped - keyed by the first address, never adjacent.
        std::map<uintptr_t, uintptr_t> Holes;
    };

    using TRegions = std::map<uintptr_t, TRegion>;

    TRegions::iterator FindRegion(uintptr_t begin, uintptr_t end) {
        auto it = Regions_.upper_bound(begin);
        if (it == Regions_.begin()) {
            return Regions_.end();
        }

        --it;
        return end <= it->second.End ? it : Regions_.end();
    }

    static int DontNeed(uintptr_t begin, size_t size) noexcept {
        void* addr = reinterpret_cast<void*>(begin);

        // Unlock the memory in case somewhere was called `mlockall(MCL_FUTURE)` - `madvise()`
        // refuses to drop the pages of a locked region.
        if (::munlock(addr, size) == -1 && LastSystemError() == ENOMEM) {
            // Unlocking a region would result in the total number of mappings with distinct
            // attributes (e.g. locked versus unlocked) exceeding the allowed maximum.
            // NOTE: `madvise(MADV_DONTNEED)` can't return ENOMEM, so report the failure here.
            // The other errors either mean the region was simply not locked (EAGAIN, EPERM) or
            // are reported by the `madvise()` call below as well (EINVAL).
            return -1;
        }

        /**
            There is at least a couple of drawbacks of using madvise instead of munmap:
            - more potential for use-after-free and memory corruption since we still may access
              unneeded regions by mistake,
            - actual RSS memory may be freed later after kernel gets some memory-pressure, and it
              may confuse system monitoring tools.

            But also there is a huge advantage: the number of memory maps used by process doesn't
            increase because of the "holes".
         */
        return ::madvise(addr, size, MADV_DONTNEED);
    }

    TRegions Regions_;
    size_t TotalSize_ = 0;
    mutable std::mutex Lock_;
};

TMappedRegions& MappedRegions() {
    // Intentionally never destroyed: the global page pools release their pages at the process exit,
    // i.e. after the singletons (i.e. this one as well) have already been destroyed.
    static auto* regions = new TMappedRegions();
    return *regions;
}

} // namespace
#endif

#ifdef _win_
    #define MAP_FAILED (void*)(-1)
inline void* TSystemMmap::Mmap(size_t size)
{
    if (auto res = ::VirtualAlloc(0, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)) {
        return res;
    } else {
        return MAP_FAILED;
    }
}

inline int TSystemMmap::Munmap(void* addr, size_t size) noexcept {
    Y_ABORT_UNLESS(AlignUp(addr, SYS_PAGE_SIZE) == addr, "Got unaligned address");
    Y_ABORT_UNLESS(AlignUp(size, SYS_PAGE_SIZE) == size, "Got unaligned size");
    return !::VirtualFree(addr, size, MEM_DECOMMIT);
}
#else
inline void* TSystemMmap::Mmap(size_t size)
{
    void* res = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
    if (Y_LIKELY(MAP_FAILED != res)) {
        MappedRegions().Register(res, size);
    }
    return res;
}

inline int TSystemMmap::Munmap(void* addr, size_t size) noexcept {
    Y_DEBUG_ABORT_UNLESS(AlignUp(addr, SYS_PAGE_SIZE) == addr, "Got unaligned address");
    Y_DEBUG_ABORT_UNLESS(AlignUp(size, SYS_PAGE_SIZE) == size, "Got unaligned size");

    /**
        The main source of the growth of number of memory regions is a clean-up of freed pages from
        page pools: unmapping a single page out of a larger mapping punches a hole in it and splits
        it in two. `TMappedRegions` tracks what we have mapped, so that such a range is really
        unmapped only when that doesn't split anything - and is merely dropped from the resident
        memory otherwise. Thus `TAlignedPagePool::DoCleanupGlobalFreeList()` can be safely invoked
        whenever we want it, and still the address space (and the page tables) is given back to the
        system once the whole region gets free.
     */
    return MappedRegions().Release(addr, size);
}
#endif

TSystemMmap& TSystemMmap::GetInstance() {
    return *Singleton<TSystemMmap>();
}

TFakeMmap& TFakeMmap::GetInstance() {
    return *Singleton<TFakeMmap>();
}

void* TFakeMmap::Mmap(size_t size) {
    Y_DEBUG_ABORT_UNLESS(OnMmap, "mmap function must be provided");
    return OnMmap(size);
}

int TFakeMmap::Munmap(void* addr, size_t size) noexcept {
    return NYql::WithAbortOnException([&] {
        if (OnMunmap) {
            OnMunmap(addr, size);
        }
        return 0;
    }, "TFakeMmap::Munmap");
}

TAlignedPagePoolCounters::TAlignedPagePoolCounters(::NMonitoring::TDynamicCounterPtr countersRoot, const TString& name) {
    if (!countersRoot || name.empty()) {
        return;
    }
    ::NMonitoring::TDynamicCounterPtr subGroup = countersRoot->GetSubgroup("counters", "utils")->GetSubgroup("subsystem", "mkqlalloc");
    TotalBytesAllocatedCntr = subGroup->GetCounter(name + "/TotalBytesAllocated");
    AllocationsCntr = subGroup->GetCounter(name + "/Allocations", /*derivative=*/true);
    PoolsCntr = subGroup->GetCounter(name + "/Pools", /*derivative=*/true);
    LostPagesBytesFreeCntr = subGroup->GetCounter(name + "/LostPagesBytesFreed", /*derivative=*/true);
}

template <typename T>
TAlignedPagePoolImpl<T>::~TAlignedPagePoolImpl() {
    NYql::WithAbortOnException([&] {
        if (CheckLostMem_ && !UncaughtException()) {
            Y_DEBUG_ABORT_UNLESS(TotalAllocated_ == FreePages_.size() * POOL_PAGE_SIZE,
                                 "memory leak; Expected %ld, actual %ld (%ld page(s), %ld offloaded); allocator created at: %s",
                                 TotalAllocated_, FreePages_.size() * POOL_PAGE_SIZE,
                                 FreePages_.size(), OffloadedActiveBytes_, GetDebugInfo().data());
            Y_DEBUG_ABORT_UNLESS(OffloadedActiveBytes_ == 0, "offloaded: %ld", OffloadedActiveBytes_);
        }

        size_t activeBlocksSize = 0;
        for (auto it = ActiveBlocks_.cbegin(); ActiveBlocks_.cend() != it; ActiveBlocks_.erase(it++)) {
            activeBlocksSize += it->second;

            if (Y_UNLIKELY(IsDefaultAllocatorUsed())) {
                ReturnBlock(it->first, it->second);
                return;
            }

            Free(it->first, it->second);
        }

        if (activeBlocksSize > 0 || FreePages_.size() != AllPages_.size() || OffloadedActiveBytes_) {
            if (Counters_.LostPagesBytesFreeCntr) {
                (*Counters_.LostPagesBytesFreeCntr) += OffloadedActiveBytes_ + activeBlocksSize + (AllPages_.size() - FreePages_.size()) * POOL_PAGE_SIZE;
            }
        }

        Y_DEBUG_ABORT_UNLESS(TotalAllocated_ == AllPages_.size() * POOL_PAGE_SIZE + OffloadedActiveBytes_,
                             "Expected %ld, actual %ld (%ld page(s))", TotalAllocated_,
                             AllPages_.size() * POOL_PAGE_SIZE + OffloadedActiveBytes_, AllPages_.size());

        for (auto& ptr : AllPages_) {
            TGlobalPools<T, false>::Instance().PushPage(0, ptr);
        }

        if (Counters_.TotalBytesAllocatedCntr) {
            (*Counters_.TotalBytesAllocatedCntr) -= TotalAllocated_;
        }
        if (Counters_.PoolsCntr) {
            --(*Counters_.PoolsCntr);
        }
        TotalAllocated_ = 0;
    }, "TAlignedPagePoolImpl");
}

template <typename T>
void TAlignedPagePoolImpl<T>::ReleaseFreePages() {
    TotalAllocated_ -= FreePages_.size() * POOL_PAGE_SIZE;
    if (Counters_.TotalBytesAllocatedCntr) {
        (*Counters_.TotalBytesAllocatedCntr) -= FreePages_.size() * POOL_PAGE_SIZE;
    }

    for (; !FreePages_.empty(); FreePages_.pop()) {
        AllPages_.erase(FreePages_.top());
        TGlobalPools<T, false>::Instance().PushPage(0, FreePages_.top());
    }
}

template <typename T>
void TAlignedPagePoolImpl<T>::OffloadAlloc(ui64 size) {
    if (Limit_ && TotalAllocated_ + size > Limit_ && !TryIncreaseLimit(TotalAllocated_ + size)) {
        throw TMemoryLimitExceededException(); // NOLINT(hicpp-exception-baseclass)
    }

    if (AllocNotifyCallback_) {
        if (AllocNotifyCurrentBytes_ > AllocNotifyBytes_) {
            AllocNotifyCallback_();
            AllocNotifyCurrentBytes_ = 0;
        }
    }

    ++OffloadedAllocCount_;
    OffloadedBytes_ += size;
    OffloadedActiveBytes_ += size;
    TotalAllocated_ += size;
    if (AllocNotifyCallback_) {
        AllocNotifyCurrentBytes_ += size;
    }
    if (Counters_.TotalBytesAllocatedCntr) {
        (*Counters_.TotalBytesAllocatedCntr) += size;
    }

    if (Counters_.AllocationsCntr) {
        ++(*Counters_.AllocationsCntr);
    }

    UpdatePeaks();
}

template <typename T>
void TAlignedPagePoolImpl<T>::OffloadFree(ui64 size) noexcept {
    TotalAllocated_ -= size;
    OffloadedActiveBytes_ -= size;
    if (Counters_.TotalBytesAllocatedCntr) {
        (*Counters_.TotalBytesAllocatedCntr) -= size;
    }
}

template <typename T>
void* TAlignedPagePoolImpl<T>::GetPageImpl() {
    ++PageAllocCount_;
    if (!FreePages_.empty()) {
        ++PageHitCount_;
        const auto res = FreePages_.top();
        FreePages_.pop();
        return res;
    }

    if (Limit_ && TotalAllocated_ + POOL_PAGE_SIZE > Limit_ && !TryIncreaseLimit(TotalAllocated_ + POOL_PAGE_SIZE)) {
        throw TMemoryLimitExceededException(); // NOLINT(hicpp-exception-baseclass)
    }

    if (Y_LIKELY(!IsDefaultAllocatorUsed())) {
        if (const auto ptr = TGlobalPools<T, false>::Instance().Get(0).GetPage()) {
            TotalAllocated_ += POOL_PAGE_SIZE;
            if (AllocNotifyCallback_) {
                AllocNotifyCurrentBytes_ += POOL_PAGE_SIZE;
            }
            if (Counters_.TotalBytesAllocatedCntr) {
                (*Counters_.TotalBytesAllocatedCntr) += POOL_PAGE_SIZE;
            }
            ++PageGlobalHitCount_;
            AllPages_.emplace(ptr);

            UpdatePeaks();
            return ptr;
        }

        ++PageMissCount_;
    }

    void* res;
    if (Y_UNLIKELY(IsDefaultAllocatorUsed())) {
        res = GetBlock(POOL_PAGE_SIZE);
    } else {
        res = Alloc(POOL_PAGE_SIZE);
    }
    AllPages_.emplace(res);

    return res;
}

template <typename T>
void* TAlignedPagePoolImpl<T>::GetPage() {
    auto* page = GetPageImpl();
    NYql::NUdf::SanitizerMakeRegionInaccessible(page, POOL_PAGE_SIZE);
    return page;
};

template <typename T>
void TAlignedPagePoolImpl<T>::ReturnPage(void* addr) {
    if (Y_UNLIKELY(IsDefaultAllocatorUsed())) {
        ReturnBlock(addr, POOL_PAGE_SIZE);
        AllPages_.erase(addr);
        return;
    }

    NYql::NUdf::SanitizerMakeRegionInaccessible(addr, POOL_PAGE_SIZE);
    Y_DEBUG_ABORT_UNLESS(AllPages_.find(addr) != AllPages_.end());
    FreePages_.emplace(addr);
}

template <typename T>
void* TAlignedPagePoolImpl<T>::GetBlock(size_t size) {
    Y_DEBUG_ABORT_UNLESS(size >= POOL_PAGE_SIZE);

    if (Y_UNLIKELY(IsDefaultAllocatorUsed())) {
        OffloadAlloc(size);
        auto ret = malloc(size);
        if (!ret) {
            // NOLINTNEXTLINE(hicpp-exception-baseclass)
            throw TMemoryLimitExceededException();
        }

        return ret;
    }
    auto* block = GetBlockImpl(size);
    NYql::NUdf::SanitizerMakeRegionInaccessible(block, size);
    return block;
}

template <typename T>
void TAlignedPagePoolImpl<T>::ReturnBlock(void* ptr, size_t size) {
    Y_DEBUG_ABORT_UNLESS(size >= POOL_PAGE_SIZE);

    if (Y_UNLIKELY(IsDefaultAllocatorUsed())) {
        OffloadFree(size);
        free(ptr);
        UpdateMemoryYellowZone();
        return;
    }

    if (size == POOL_PAGE_SIZE) {
        ReturnPage(ptr);
    } else {
        Free(ptr, size);
        Y_DEBUG_ABORT_UNLESS(ActiveBlocks_.erase(ptr));
    }
    UpdateMemoryYellowZone();
}

template <typename T>
void* TAlignedPagePoolImpl<T>::Alloc(size_t size) {
    void* res = nullptr;
    size = AlignUp(size, SYS_PAGE_SIZE);

    if (Limit_ && TotalAllocated_ + size > Limit_ && !TryIncreaseLimit(TotalAllocated_ + size)) {
        throw TMemoryLimitExceededException(); // NOLINT(hicpp-exception-baseclass)
    }

    if (AllocNotifyCallback_) {
        if (AllocNotifyCurrentBytes_ > AllocNotifyBytes_) {
            AllocNotifyCallback_();
            AllocNotifyCurrentBytes_ = 0;
        }
    }

    auto& globalPool = TGlobalPools<T, false>::Instance();

    if (size > POOL_PAGE_SIZE && size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level >= 1 && level <= MidLevels);
        if (res = globalPool.Get(level).GetPage()) {
            TotalAllocated_ += size;
            if (AllocNotifyCallback_) {
                AllocNotifyCurrentBytes_ += size;
            }
            if (Counters_.TotalBytesAllocatedCntr) {
                (*Counters_.TotalBytesAllocatedCntr) += size;
            }
            ++PageGlobalHitCount_;
        } else {
            ++PageMissCount_;
        }
    }

    if (!res) {
        auto allocSize = size + ALLOC_AHEAD_PAGES * POOL_PAGE_SIZE;
        void* mem = globalPool.DoMmap(allocSize);
        if (Y_UNLIKELY(MAP_FAILED == mem)) {
            TStringStream mmaps;
            const auto lastError = LastSystemError();
            if (lastError == ENOMEM) {
                mmaps << GetMemoryMapsString();
            }

            ythrow yexception() << "Mmap failed to allocate " << (size + POOL_PAGE_SIZE) << " bytes: "
                                << LastSystemErrorText(lastError) << mmaps.Str();
        }

        res = AlignUp(mem, POOL_PAGE_SIZE);
        const size_t off = reinterpret_cast<intptr_t>(res) - reinterpret_cast<intptr_t>(mem);
        if (Y_UNLIKELY(off)) {
            // unmap prefix
            globalPool.DoMunmap(mem, off);
        }
        // Extra space is also page-aligned. Put it to the free page list
        auto alignedSize = AlignUp(size, POOL_PAGE_SIZE);
        ui64 extraPages = (allocSize - off - alignedSize) / POOL_PAGE_SIZE;
        ui64 tail = (allocSize - off - alignedSize) % POOL_PAGE_SIZE;
        auto extraPage = reinterpret_cast<ui8*>(res) + alignedSize;
        for (ui64 i = 0; i < extraPages; ++i) {
            AllPages_.emplace(extraPage);
            FreePages_.emplace(extraPage);
            extraPage += POOL_PAGE_SIZE;
        }
        if (size != alignedSize) {
            // unmap unaligned hole
            globalPool.DoMunmap(reinterpret_cast<ui8*>(res) + size, alignedSize - size);
        }

        if (tail) {
            // unmap suffix
            Y_DEBUG_ABORT_UNLESS(extraPage + tail <= reinterpret_cast<ui8*>(mem) + size + ALLOC_AHEAD_PAGES * POOL_PAGE_SIZE);
            globalPool.DoMunmap(extraPage, tail);
        }

        auto extraSize = extraPages * POOL_PAGE_SIZE;
        auto totalSize = size + extraSize;
        TotalAllocated_ += totalSize;
        if (AllocNotifyCallback_) {
            AllocNotifyCurrentBytes_ += totalSize;
        }
        if (Counters_.TotalBytesAllocatedCntr) {
            (*Counters_.TotalBytesAllocatedCntr) += totalSize;
        }
    }

    if (Counters_.AllocationsCntr) {
        ++(*Counters_.AllocationsCntr);
    }
    ++AllocCount_;
    UpdatePeaks();
    return res;
}

template <typename T>
void TAlignedPagePoolImpl<T>::Free(void* ptr, size_t size) {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size <= MaxMidSize) {
        size = FastClp2(size);
    }
    if (size <= MaxMidSize) {
        auto level = LeastSignificantBit(size) - LeastSignificantBit(POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level >= 1 && level <= MidLevels);
        TGlobalPools<T, false>::Instance().PushPage(level, ptr);
    } else {
        TGlobalPools<T, false>::Instance().DoMunmap(ptr, size);
    }

    Y_DEBUG_ABORT_UNLESS(TotalAllocated_ >= size);
    TotalAllocated_ -= size;
    if (Counters_.TotalBytesAllocatedCntr) {
        (*Counters_.TotalBytesAllocatedCntr) -= size;
    }
}

template <typename T>
void TAlignedPagePoolImpl<T>::DoCleanupGlobalFreeList(ui64 targetSize) {
    TGlobalPools<T, true>::Instance().DoCleanupFreeList(targetSize);
    TGlobalPools<T, false>::Instance().DoCleanupFreeList(targetSize);
}

template <typename T>
void TAlignedPagePoolImpl<T>::UpdateMemoryYellowZone() {
    if (Limit_ == 0) {
        return;
    }
    if (IsMemoryYellowZoneForcefullyChanged_) {
        return;
    }
    if (IncreaseMemoryLimitCallback_ && !IsMaximumLimitValueReached_) {
        return;
    }

    ui8 usedMemoryPercent = 100 * GetUsed() / Limit_;
    if (usedMemoryPercent >= EnableMemoryYellowZoneThreshold_) {
        IsMemoryYellowZoneReached_ = true;
    } else if (usedMemoryPercent <= DisableMemoryYellowZoneThreshold_) {
        IsMemoryYellowZoneReached_ = false;
    }
}

template <typename T>
bool TAlignedPagePoolImpl<T>::TryIncreaseLimit(ui64 required) {
    if (!IncreaseMemoryLimitCallback_) {
        return false;
    }
    IncreaseMemoryLimitCallback_(Limit_, required);
    return Limit_ >= required;
}

template <typename T>
void* TAlignedPagePoolImpl<T>::GetBlockImpl(size_t size) {
    if (size == POOL_PAGE_SIZE) {
        return GetPage();
    } else {
        const auto ptr = Alloc(size);
        Y_DEBUG_ABORT_UNLESS(ActiveBlocks_.emplace(ptr, size).second);
        return ptr;
    }
}

template <typename T>
ui64 TAlignedPagePoolImpl<T>::GetGlobalPagePoolSize() {
    ui64 size = 0;
    for (size_t level = 0; level <= MidLevels; ++level) {
        size += TGlobalPools<T, false>::Instance().Get(level).GetSize();
    }
    return size;
}

template <typename T>
void TAlignedPagePoolImpl<T>::PrintStat(size_t usedPages, IOutputStream& out) const {
    usedPages += GetFreePageCount();
    out << "Count of free pages: " << GetFreePageCount() << Endl;
    out << "Allocated for blocks: " << (GetAllocated() - usedPages * POOL_PAGE_SIZE) << Endl;
    out << "Total allocated by lists: " << GetAllocated() << Endl;
}

template <typename T>
void TAlignedPagePoolImpl<T>::ResetGlobalsUT()
{
    TGlobalPools<T, false>::Instance().Reset();
}

#if defined(ALLOW_DEFAULT_ALLOCATOR)
// static
template <typename T>
bool TAlignedPagePoolImpl<T>::IsDefaultAllocatorUsed() {
    return IsDefaultAllocator;
}
#endif
// static
template <typename T>
bool TAlignedPagePoolImpl<T>::IsDefaultArrowAllocatorUsed() {
    return IsDefaultArrowAllocator || IsDefaultAllocatorUsed();
}

template class TAlignedPagePoolImpl<>;
template class TAlignedPagePoolImpl<TFakeMmap>;

template <typename TMmap>
void* GetAlignedPage(ui64 size) {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size < TAlignedPagePool::POOL_PAGE_SIZE) {
        size = TAlignedPagePool::POOL_PAGE_SIZE;
    }

    auto& pool = TGlobalPools<TMmap, true>::Instance();

    if (size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        if (auto res = pool.Get(level).GetPage()) {
            return res;
        }
    }

    auto allocSize = Max<ui64>(MaxMidSize, size);
    void* mem = pool.DoMmap(allocSize);
    if (Y_UNLIKELY(MAP_FAILED == mem)) {
        TStringStream mmaps;
        const auto lastError = LastSystemError();
        if (lastError == ENOMEM) {
            mmaps << GetMemoryMapsString();
        }

        ythrow yexception() << "Mmap failed to allocate " << allocSize << " bytes: " << LastSystemErrorText(lastError) << mmaps.Str();
    }

    if (size < MaxMidSize) {
        // push extra allocated pages to cache
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        ui8* ptr = (ui8*)mem + size;
        ui8* const end = (ui8*)mem + MaxMidSize;
        while (ptr < end) {
            pool.PushPage(level, ptr);
            ptr += size;
        }
    }

    return mem;
}

template <typename TMmap>
void* GetAlignedPage() {
    const auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto& globalPool = TGlobalPools<TMmap, false>::Instance();

    if (auto* page = globalPool.Get(0).GetPage()) {
        return page;
    }

    // Map a whole batch of pages at once and cache the rest of them: mapping the pages one by one
    // would spend a memory mapping (and a couple of syscalls) on the alignment of every single page.
    constexpr ui64 batchPages = TAlignedPagePool::ALLOC_AHEAD_PAGES + 1;
    const auto allocSize = (batchPages + 1) * size; // one extra page to align the batch within

    void* unalignedPtr = globalPool.DoMmap(allocSize);
    if (Y_UNLIKELY(MAP_FAILED == unalignedPtr)) {
        TStringStream mmaps;
        const auto lastError = LastSystemError();
        if (lastError == ENOMEM) {
            mmaps << GetMemoryMapsString();
        }

        ythrow yexception() << "Mmap failed to allocate " << allocSize << " bytes: "
                            << LastSystemErrorText(lastError) << mmaps.Str();
    }

    void* page = AlignUp(unalignedPtr, size);

    // Unmap unaligned prefix before offset and tail after the batch. Both are at the boundary of
    // the fresh mapping, so neither of them splits it in two.
    const size_t offset = (intptr_t)page - (intptr_t)unalignedPtr;
    if (Y_UNLIKELY(offset)) {
        globalPool.DoMunmap(unalignedPtr, offset);
    }
    globalPool.DoMunmap((ui8*)page + batchPages * size, size - offset);

    for (ui64 i = 1; i < batchPages; ++i) {
        globalPool.PushPage(0, (ui8*)page + i * size);
    }

    return page;
}

template <typename TMmap>
void ReleaseAlignedPage(void* mem, ui64 size) {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size < TAlignedPagePool::POOL_PAGE_SIZE) {
        size = TAlignedPagePool::POOL_PAGE_SIZE;
    }

    if (size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        TGlobalPools<TMmap, true>::Instance().PushPage(level, mem);
        return;
    }
    TGlobalPools<TMmap, true>::Instance().DoMunmap(mem, size);
}

template <typename TMmap>
void ReleaseAlignedPage(void* mem) {
    TGlobalPools<TMmap, false>::Instance().PushPage(0, mem);
}

template <typename TMmap>
i64 GetTotalMmapedBytes() {
    return TGlobalPools<TMmap, true>::Instance().GetTotalMmappedBytes() + TGlobalPools<TMmap, false>::Instance().GetTotalMmappedBytes();
}

template <typename TMmap>
i64 GetTotalFreeListBytes() {
    return TGlobalPools<TMmap, true>::Instance().GetTotalFreeListBytes() + TGlobalPools<TMmap, false>::Instance().GetTotalFreeListBytes();
}

template i64 GetTotalMmapedBytes<>();
template i64 GetTotalMmapedBytes<TFakeMmap>();

template i64 GetTotalFreeListBytes<>();
template i64 GetTotalFreeListBytes<TFakeMmap>();

template void* GetAlignedPage<>(ui64);
template void* GetAlignedPage<TFakeMmap>(ui64);

template void* GetAlignedPage<>();
template void* GetAlignedPage<TFakeMmap>();

template void ReleaseAlignedPage<>(void*, ui64);
template void ReleaseAlignedPage<TFakeMmap>(void*, ui64);

template void ReleaseAlignedPage<>(void*);
template void ReleaseAlignedPage<TFakeMmap>(void*);

size_t GetMappedRegionsCount() {
#ifdef _win_
    return 0;
#else
    return MappedRegions().GetCount();
#endif
}

size_t GetMappedAddressSpaceSize() {
#ifdef _win_
    return 0;
#else
    return MappedRegions().GetTotalSize();
#endif
}

size_t GetMemoryMapsCount() {
    size_t lineCount = 0;
    TString line;
#if defined(_unix_) && !defined(_darwin_)
    TFileInput file("/proc/self/maps");
    while (file.ReadLine(line)) {
        ++lineCount;
    }
#endif
    return lineCount;
}

} // namespace NKikimr
