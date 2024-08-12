#pragma once

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>
#include <util/system/defaults.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <type_traits>
#include <stack>
#include <vector>
#include <unordered_set>
#include <unordered_map>

namespace NKikimr {

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
class TMemoryLimitExceededException {
public:
    virtual ~TMemoryLimitExceededException() = default;
};

class TSystemMmap {
public:
    static void* Mmap(size_t size);
    static int Munmap(void* addr, size_t size);
};

class TFakeAlignedMmap {
public:
    static std::function<void(size_t size)> OnMmap;
    static std::function<void(void* addr, size_t size)> OnMunmap;

    static void* Mmap(size_t size);
    static int Munmap(void* addr, size_t size);
};

class TFakeUnalignedMmap {
public:
    static std::function<void(size_t size)> OnMmap;
    static std::function<void(void* addr, size_t size)> OnMunmap;

    static void* Mmap(size_t size);
    static int Munmap(void* addr, size_t size);
};

template<typename TMmap = TSystemMmap>
class TAlignedPagePoolImpl {
public:
    static constexpr ui64 POOL_PAGE_SIZE = 1ULL << 16; // 64k
    static constexpr ui64 PAGE_ADDR_MASK = ~(POOL_PAGE_SIZE - 1);
    static constexpr ui64 ALLOC_AHEAD_PAGES = 31;

    explicit TAlignedPagePoolImpl(const TSourceLocation& location,
            const TAlignedPagePoolCounters& counters = TAlignedPagePoolCounters())
        : Counters(counters)
        , DebugInfo(location)
    {
        if (Counters.PoolsCntr) {
            ++(*Counters.PoolsCntr);
        }
    }

    TAlignedPagePoolImpl(const TAlignedPagePoolImpl&) = delete;
    TAlignedPagePoolImpl(TAlignedPagePoolImpl&& other) = delete;

    TAlignedPagePoolImpl& operator = (const TAlignedPagePoolImpl&) = delete;
    TAlignedPagePoolImpl& operator = (TAlignedPagePoolImpl&& other) = delete;

    ~TAlignedPagePoolImpl();

    inline size_t GetAllocated() const noexcept {
        return TotalAllocated;
    }

    inline size_t GetUsed() const noexcept {
        return TotalAllocated - GetFreePageCount() * POOL_PAGE_SIZE;
    }

    inline size_t GetFreePageCount() const noexcept {
        return FreePages.size();
    }

    static inline const void* GetPageStart(const void* addr) noexcept {
        return reinterpret_cast<const void*>(reinterpret_cast<uintptr_t>(addr) & PAGE_ADDR_MASK);
    }

    static inline void* GetPageStart(void* addr) noexcept {
        return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(addr) & PAGE_ADDR_MASK);
    }

    void* GetPage();

    void ReturnPage(void* addr) noexcept;

    void Swap(TAlignedPagePoolImpl& other) {
        DoSwap(FreePages, other.FreePages);
        DoSwap(AllPages, other.AllPages);
        DoSwap(ActiveBlocks, other.ActiveBlocks);
        DoSwap(TotalAllocated, other.TotalAllocated);
        DoSwap(PeakAllocated, other.PeakAllocated);
        DoSwap(PeakUsed, other.PeakUsed);
        DoSwap(Limit, other.Limit);
        DoSwap(AllocCount, other.AllocCount);
        DoSwap(PageAllocCount, other.PageAllocCount);
        DoSwap(PageHitCount, other.PageHitCount);
        DoSwap(PageGlobalHitCount, other.PageGlobalHitCount);
        DoSwap(PageMissCount, other.PageMissCount);
        DoSwap(OffloadedAllocCount, other.OffloadedAllocCount);
        DoSwap(OffloadedBytes, other.OffloadedBytes);
        DoSwap(OffloadedActiveBytes, other.OffloadedActiveBytes);
        DoSwap(Counters, other.Counters);
        DoSwap(CheckLostMem, other.CheckLostMem);
        DoSwap(AllocNotifyCallback, other.AllocNotifyCallback);
        DoSwap(IncreaseMemoryLimitCallback, other.IncreaseMemoryLimitCallback);
    }

    void PrintStat(size_t usedPages, IOutputStream& out) const;

    TString GetDebugInfo() const {
        return ToString(DebugInfo);
    }

    void* GetBlock(size_t size);

    void ReturnBlock(void* ptr, size_t size) noexcept;

    size_t GetPeakAllocated() const noexcept {
        return PeakAllocated;
    }

    size_t GetPeakUsed() const noexcept {
        return PeakUsed;
    }

    ui64 GetAllocCount() const noexcept {
        return AllocCount;
    }

    ui64 GetPageAllocCount() const noexcept {
        return PageAllocCount;
    }

    ui64 GetPageHitCount() const noexcept {
        return PageHitCount;
    }

    ui64 GetPageGlobalHitCount() const noexcept {
        return PageGlobalHitCount;
    }

    ui64 GetPageMissCount() const noexcept {
        return PageMissCount;
    }

    ui64 GetOffloadedAllocCount() const noexcept {
        return OffloadedAllocCount;
    }

    ui64 GetOffloadedBytes() const noexcept {
        return OffloadedBytes;
    }

    void OffloadAlloc(ui64 size);
    void OffloadFree(ui64 size) noexcept;

    static ui64 GetGlobalPagePoolSize();

    ui64 GetLimit() const noexcept {
        return Limit;
    }

    void SetLimit(size_t limit) noexcept {
        Limit = limit;
    }

    void ReleaseFreePages();

    void DisableStrictAllocationCheck() noexcept {
        CheckLostMem = false;
    }

    using TAllocNotifyCallback = std::function<void()>;
    void SetAllocNotifyCallback(TAllocNotifyCallback&& callback, ui64 notifyBytes = 0) {
        AllocNotifyCallback = std::move(callback);
        AllocNotifyBytes = notifyBytes;
        AllocNotifyCurrentBytes = 0;
    }

    using TIncreaseMemoryLimitCallback = std::function<void(ui64 currentLimit, ui64 required)>;

    void SetIncreaseMemoryLimitCallback(TIncreaseMemoryLimitCallback&& callback) {
        IncreaseMemoryLimitCallback = std::move(callback);
    }

    static void ResetGlobalsUT();

    void SetMaximumLimitValueReached(bool isReached) noexcept {
        IsMaximumLimitValueReached = isReached;
    }

    bool IsMemoryYellowZoneEnabled() const noexcept {
        return IsMemoryYellowZoneReached;
    }

    void ForcefullySetMemoryYellowZone(bool isEnabled) noexcept {
        IsMemoryYellowZoneReached = isEnabled;
        IsMemoryYellowZoneForcefullyChanged = true;
    }

protected:
    void* Alloc(size_t size);
    void Free(void* ptr, size_t size) noexcept;

    void UpdatePeaks() {
        PeakAllocated = Max(PeakAllocated, GetAllocated());
        PeakUsed = Max(PeakUsed, GetUsed());

        UpdateMemoryYellowZone();
    }

    void UpdateMemoryYellowZone();

    bool TryIncreaseLimit(ui64 required);

protected:
    std::stack<void*, std::vector<void*>> FreePages;
    std::unordered_set<void*> AllPages;
    std::unordered_map<void*, size_t> ActiveBlocks;
    size_t TotalAllocated = 0;
    size_t PeakAllocated = 0;
    size_t PeakUsed = 0;
    size_t Limit = 0;

    ui64 AllocCount = 0;
    ui64 PageAllocCount = 0;
    ui64 PageHitCount = 0;
    ui64 PageGlobalHitCount = 0;
    ui64 PageMissCount = 0;

    ui64 OffloadedAllocCount = 0;
    ui64 OffloadedBytes = 0;
    ui64 OffloadedActiveBytes = 0;

    TAlignedPagePoolCounters Counters;
    bool CheckLostMem = true;

    TAllocNotifyCallback AllocNotifyCallback;
    ui64 AllocNotifyBytes = 0;
    ui64 AllocNotifyCurrentBytes = 0;

    TIncreaseMemoryLimitCallback IncreaseMemoryLimitCallback;
    const TSourceLocation DebugInfo;

    // Indicates when memory limit is almost reached.
    bool IsMemoryYellowZoneReached = false;
    // Indicates that memory yellow zone was enabled or disabled forcefully.
    // If the value of this variable is true, then the limits specified below will not be applied and
    // changing the value can only be done manually.
    bool IsMemoryYellowZoneForcefullyChanged = false;
    // This theshold is used to determine is memory limit is almost reached.
    // If TIncreaseMemoryLimitCallback is set this thresholds should be ignored.
    // The yellow zone turns on when memory consumption reaches 80% and turns off when consumption drops below 50%.
    const ui8 EnableMemoryYellowZoneThreshold = 80;
    const ui8 DisableMemoryYellowZoneThreshold = 50;

    // This flag indicates that value of memory limit reached it's maximum.
    // Next TryIncreaseLimit call most likely will return false.
    bool IsMaximumLimitValueReached = false;
};

using TAlignedPagePool = TAlignedPagePoolImpl<>;

template<typename TMmap = TSystemMmap>
void* GetAlignedPage(ui64 size);

template<typename TMmap = TSystemMmap>
void ReleaseAlignedPage(void* mem, ui64 size);

} // NKikimr
