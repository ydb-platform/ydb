#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

#include <stack>
#include <queue>

#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace NKikimr {

#if defined(ALLOW_DEFAULT_ALLOCATOR)
// By default the default allocator is not used unless PROFILE_MEMORY_ALLOCATIONS is defined.
// Call this method once at the start of the process - to enable usage of default allocator.
void UseDefaultAllocator();
#endif
void UseDefaultArrowAllocator();

struct TAlignedPagePoolCounters {
    explicit TAlignedPagePoolCounters(::NMonitoring::TDynamicCounterPtr countersRoot = nullptr, const TString& name = TString());

    ::NMonitoring::TDynamicCounters::TCounterPtr TotalBytesAllocatedCntr;
    ::NMonitoring::TDynamicCounters::TCounterPtr AllocationsCntr;
    ::NMonitoring::TDynamicCounters::TCounterPtr PoolsCntr;
    ::NMonitoring::TDynamicCounters::TCounterPtr LostPagesBytesFreeCntr;

    void Swap(TAlignedPagePoolCounters& other) {
        DoSwap(TotalBytesAllocatedCntr, other.TotalBytesAllocatedCntr);
        DoSwap(AllocationsCntr, other.AllocationsCntr);
        DoSwap(PoolsCntr, other.PoolsCntr);
        DoSwap(LostPagesBytesFreeCntr, other.LostPagesBytesFreeCntr);
    }
};

// NOTE: We intentionally avoid inheritance from std::exception here to make it harder
// to catch this exception in UDFs code, so we can handle it in the host.
// NOLINTNEXTLINE(hicpp-exception-baseclass)
class TMemoryLimitExceededException {
public:
    virtual ~TMemoryLimitExceededException() = default;
};

class TSystemMmap {
public:
    static void* Mmap(size_t size);
    static int Munmap(void* addr, size_t size) noexcept;
};

class TFakeMmap {
public:
    static std::function<void*(size_t size)> OnMmap;
    static std::function<void(void* addr, size_t size)> OnMunmap;

    static void* Mmap(size_t size);
    static int Munmap(void* addr, size_t size) noexcept;
};

template <typename TMmap = TSystemMmap>
class TAlignedPagePoolImpl {
public:
    static constexpr ui64 POOL_PAGE_SIZE = 1ULL << 16; // 64k
    static constexpr ui64 PAGE_ADDR_MASK = ~(POOL_PAGE_SIZE - 1);
    static constexpr ui64 ALLOC_AHEAD_PAGES = 31;

    explicit TAlignedPagePoolImpl(const TSourceLocation& location,
                                  const TAlignedPagePoolCounters& counters = TAlignedPagePoolCounters())
        : Counters_(counters)
        , DebugInfo_(location)
    {
        if (Counters_.PoolsCntr) {
            ++(*Counters_.PoolsCntr);
        }
    }

    TAlignedPagePoolImpl(const TAlignedPagePoolImpl&) = delete;
    TAlignedPagePoolImpl(TAlignedPagePoolImpl&& other) = delete;

    TAlignedPagePoolImpl& operator=(const TAlignedPagePoolImpl&) = delete;
    TAlignedPagePoolImpl& operator=(TAlignedPagePoolImpl&& other) = delete;

    ~TAlignedPagePoolImpl();

    inline size_t GetAllocated() const noexcept {
        return TotalAllocated_;
    }

    inline size_t GetUsed() const noexcept {
        return TotalAllocated_ - GetFreePageCount() * POOL_PAGE_SIZE;
    }

    inline size_t GetFreePageCount() const noexcept {
        return FreePages_.size();
    }

    static inline const void* GetPageStart(const void* addr) noexcept {
        return reinterpret_cast<const void*>(reinterpret_cast<uintptr_t>(addr) & PAGE_ADDR_MASK);
    }

    static inline void* GetPageStart(void* addr) noexcept {
        return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(addr) & PAGE_ADDR_MASK);
    }

    void* GetPage();

    void ReturnPage(void* addr);

    void Swap(TAlignedPagePoolImpl& other) {
        DoSwap(FreePages_, other.FreePages_);
        DoSwap(AllPages_, other.AllPages_);
        DoSwap(ActiveBlocks_, other.ActiveBlocks_);
        DoSwap(TotalAllocated_, other.TotalAllocated_);
        DoSwap(PeakAllocated_, other.PeakAllocated_);
        DoSwap(PeakUsed_, other.PeakUsed_);
        DoSwap(Limit_, other.Limit_);
        DoSwap(AllocCount_, other.AllocCount_);
        DoSwap(PageAllocCount_, other.PageAllocCount_);
        DoSwap(PageHitCount_, other.PageHitCount_);
        DoSwap(PageGlobalHitCount_, other.PageGlobalHitCount_);
        DoSwap(PageMissCount_, other.PageMissCount_);
        DoSwap(OffloadedAllocCount_, other.OffloadedAllocCount_);
        DoSwap(OffloadedBytes_, other.OffloadedBytes_);
        DoSwap(OffloadedActiveBytes_, other.OffloadedActiveBytes_);
        DoSwap(Counters_, other.Counters_);
        DoSwap(CheckLostMem_, other.CheckLostMem_);
        DoSwap(AllocNotifyCallback_, other.AllocNotifyCallback_);
        DoSwap(IncreaseMemoryLimitCallback_, other.IncreaseMemoryLimitCallback_);
    }

    void PrintStat(size_t usedPages, IOutputStream& out) const;

    TString GetDebugInfo() const {
        return ToString(DebugInfo_);
    }

    void* GetBlock(size_t size);

    void ReturnBlock(void* ptr, size_t size);

    size_t GetPeakAllocated() const noexcept {
        return PeakAllocated_;
    }

    size_t GetPeakUsed() const noexcept {
        return PeakUsed_;
    }

    ui64 GetAllocCount() const noexcept {
        return AllocCount_;
    }

    ui64 GetPageAllocCount() const noexcept {
        return PageAllocCount_;
    }

    ui64 GetPageHitCount() const noexcept {
        return PageHitCount_;
    }

    ui64 GetPageGlobalHitCount() const noexcept {
        return PageGlobalHitCount_;
    }

    ui64 GetPageMissCount() const noexcept {
        return PageMissCount_;
    }

    ui64 GetOffloadedAllocCount() const noexcept {
        return OffloadedAllocCount_;
    }

    ui64 GetOffloadedBytes() const noexcept {
        return OffloadedBytes_;
    }

    void OffloadAlloc(ui64 size);
    void OffloadFree(ui64 size) noexcept;

    static void DoCleanupGlobalFreeList(ui64 targetSize = 4 * 64 * 1024 * 1024);

    static ui64 GetGlobalPagePoolSize();

    ui64 GetLimit() const noexcept {
        return Limit_;
    }

    void SetLimit(size_t limit) noexcept {
        Limit_ = limit;
    }

    void ReleaseFreePages();

    void DisableStrictAllocationCheck() noexcept {
        CheckLostMem_ = false;
    }

    using TAllocNotifyCallback = std::function<void()>;
    void SetAllocNotifyCallback(TAllocNotifyCallback&& callback, ui64 notifyBytes = 0) {
        AllocNotifyCallback_ = std::move(callback);
        AllocNotifyBytes_ = notifyBytes;
        AllocNotifyCurrentBytes_ = 0;
    }

    using TIncreaseMemoryLimitCallback = std::function<void(ui64 currentLimit, ui64 required)>;

    void SetIncreaseMemoryLimitCallback(TIncreaseMemoryLimitCallback&& callback) {
        IncreaseMemoryLimitCallback_ = std::move(callback);
    }

    static void ResetGlobalsUT();

    void SetMaximumLimitValueReached(bool isReached) noexcept {
        IsMaximumLimitValueReached_ = isReached;
    }

    bool GetMaximumLimitValueReached() const noexcept {
        return IsMaximumLimitValueReached_;
    }

    bool IsMemoryYellowZoneEnabled() const noexcept {
        return IsMemoryYellowZoneReached_;
    }

    void ForcefullySetMemoryYellowZone(bool isEnabled) noexcept {
        IsMemoryYellowZoneReached_ = isEnabled;
        IsMemoryYellowZoneForcefullyChanged_ = true;
    }

#if defined(ALLOW_DEFAULT_ALLOCATOR)
    static bool IsDefaultAllocatorUsed();
#else
    static consteval bool IsDefaultAllocatorUsed() {
        return false;
    }
#endif
    static bool IsDefaultArrowAllocatorUsed();

protected:
    void* Alloc(size_t size);
    void Free(void* ptr, size_t size);

    void UpdatePeaks() {
        PeakAllocated_ = Max(PeakAllocated_, GetAllocated());
        PeakUsed_ = Max(PeakUsed_, GetUsed());

        UpdateMemoryYellowZone();
    }

    void UpdateMemoryYellowZone();

    bool TryIncreaseLimit(ui64 required);

    void* GetBlockImpl(size_t size);

    void* GetPageImpl();

protected:
    std::stack<void*, std::vector<void*>> FreePages_;
    std::unordered_set<void*> AllPages_;
    std::unordered_map<void*, size_t> ActiveBlocks_;
    size_t TotalAllocated_ = 0;
    size_t PeakAllocated_ = 0;
    size_t PeakUsed_ = 0;
    size_t Limit_ = 0;

    ui64 AllocCount_ = 0;
    ui64 PageAllocCount_ = 0;
    ui64 PageHitCount_ = 0;
    ui64 PageGlobalHitCount_ = 0;
    ui64 PageMissCount_ = 0;

    ui64 OffloadedAllocCount_ = 0;
    ui64 OffloadedBytes_ = 0;
    ui64 OffloadedActiveBytes_ = 0;

    TAlignedPagePoolCounters Counters_;
    bool CheckLostMem_ = true;

    TAllocNotifyCallback AllocNotifyCallback_;
    ui64 AllocNotifyBytes_ = 0;
    ui64 AllocNotifyCurrentBytes_ = 0;

    TIncreaseMemoryLimitCallback IncreaseMemoryLimitCallback_;
    const TSourceLocation DebugInfo_;

    // Indicates when memory limit is almost reached.
    bool IsMemoryYellowZoneReached_ = false;
    // Indicates that memory yellow zone was enabled or disabled forcefully.
    // If the value of this variable is true, then the limits specified below will not be applied and
    // changing the value can only be done manually.
    bool IsMemoryYellowZoneForcefullyChanged_ = false;
    // This theshold is used to determine is memory limit is almost reached.
    // If TIncreaseMemoryLimitCallback is set this thresholds should be ignored.
    // The yellow zone turns on when memory consumption reaches 80% and turns off when consumption drops below 50%.
    const ui8 EnableMemoryYellowZoneThreshold_ = 80;
    const ui8 DisableMemoryYellowZoneThreshold_ = 50;

    // This flag indicates that value of memory limit reached it's maximum.
    // Next TryIncreaseLimit call most likely will return false.
    bool IsMaximumLimitValueReached_ = false;
};

using TAlignedPagePool = TAlignedPagePoolImpl<>;

template <typename TMmap = TSystemMmap>
void* GetAlignedPage(ui64 size);

template <typename TMmap = TSystemMmap>
void* GetAlignedPage();

template <typename TMmap = TSystemMmap>
void ReleaseAlignedPage(void* mem, ui64 size);

template <typename TMmap = TSystemMmap>
void ReleaseAlignedPage(void* mem);

template <typename TMmap = TSystemMmap>
i64 GetTotalMmapedBytes();
template <typename TMmap = TSystemMmap>
i64 GetTotalFreeListBytes();

size_t GetMemoryMapsCount();

} // namespace NKikimr
