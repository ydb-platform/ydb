#include "aligned_page_pool.h"
#include "util/string/builder.h"
#include <ydb/library/actors/util/intrinsics.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/info.h>
#include <util/system/error.h>
#include <util/thread/lfstack.h>

#if defined(_win_)
#include <util/system/winint.h>
#elif defined(_unix_)
#include <sys/types.h>
#include <sys/mman.h>
#endif

namespace NKikimr {

static ui64 SYS_PAGE_SIZE = NSystemInfo::GetPageSize();

constexpr ui32 MidLevels = 10;
constexpr ui32 MaxMidSize = (1u << MidLevels) * TAlignedPagePool::POOL_PAGE_SIZE;
static_assert(MaxMidSize == 64 * 1024 * 1024, "Upper memory block 64 Mb");

namespace {

template<typename T, bool SysAlign>
class TGlobalPagePool {
public:
    TGlobalPagePool(size_t pageSize)
        : PageSize(pageSize)
    {}

    ~TGlobalPagePool() {
        void* addr = nullptr;
        while (Pages.Dequeue(&addr)) {
            FreePage(addr);
        }
    }

    void* GetPage() {
        void *page = nullptr;
        if (Pages.Dequeue(&page)) {
            AtomicDecrement(Count);
            return page;
        }

        return nullptr;
    }

    void PushPage(void* addr) {
#ifdef PROFILE_MEMORY_ALLOCATIONS
        FreePage(addr);
#else
        AtomicIncrement(Count);
        Pages.Enqueue(addr);
#endif
    }

    ui64 GetPageCount() const {
        return RelaxedLoad(&Count);
    }

    size_t GetPageSize() const {
        return PageSize;
    }

    size_t GetSize() const {
        return GetPageCount() * GetPageSize();
    }

private:
    void FreePage(void* addr) {
        auto res = T::Munmap(addr, PageSize);
        Y_DEBUG_ABORT_UNLESS(0 == res, "Munmap failed: %s", LastSystemErrorText());
    }

private:
    const size_t PageSize;
    TAtomic Count = 0;
    TLockFreeStack<void*> Pages;
};

template<typename T, bool SysAlign>
class TGlobalPools {
public:
    static TGlobalPools<T, SysAlign>& Instance() {
        return *Singleton<TGlobalPools<T, SysAlign>>();
    }

    TGlobalPagePool<T, SysAlign>& Get(ui32 index) {
        return *Pools[index];
    }

    TGlobalPools()
    {
        Reset();
    }

    void Reset()
    {
        Pools.clear();
        Pools.reserve(MidLevels + 1);
        for (ui32 i = 0; i <= MidLevels; ++i) {
            Pools.emplace_back(MakeHolder<TGlobalPagePool<T, SysAlign>>(TAlignedPagePool::POOL_PAGE_SIZE << i));
        }
    }

private:
    TVector<THolder<TGlobalPagePool<T, SysAlign>>> Pools;
};

} // unnamed

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

inline int TSystemMmap::Munmap(void* addr, size_t size)
{
    Y_ABORT_UNLESS(AlignUp(addr, SYS_PAGE_SIZE) == addr, "Got unaligned address");
    Y_ABORT_UNLESS(AlignUp(size, SYS_PAGE_SIZE) == size, "Got unaligned size");
    return !::VirtualFree(addr, size, MEM_DECOMMIT);
}
#else
inline void* TSystemMmap::Mmap(size_t size)
{
    return ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
}

inline int TSystemMmap::Munmap(void* addr, size_t size)
{
    Y_DEBUG_ABORT_UNLESS(AlignUp(addr, SYS_PAGE_SIZE) == addr, "Got unaligned address");
    Y_DEBUG_ABORT_UNLESS(AlignUp(size, SYS_PAGE_SIZE) == size, "Got unaligned size");
    return ::munmap(addr, size);
}
#endif

std::function<void(size_t size)> TFakeAlignedMmap::OnMmap = {};
std::function<void(void* addr, size_t size)> TFakeAlignedMmap::OnMunmap = {};

void* TFakeAlignedMmap::Mmap(size_t size)
{
    if (OnMmap) {
        OnMmap(size);
    }
    return reinterpret_cast<void*>(TAlignedPagePool::POOL_PAGE_SIZE);
}

int TFakeAlignedMmap::Munmap(void* addr, size_t size)
{
    if (OnMunmap) {
        OnMunmap(addr, size);
    }
    return 0;
}

std::function<void(size_t size)> TFakeUnalignedMmap::OnMmap = {};
std::function<void(void* addr, size_t size)> TFakeUnalignedMmap::OnMunmap = {};

void* TFakeUnalignedMmap::Mmap(size_t size)
{
    if (OnMmap) {
        OnMmap(size);
    }
    return reinterpret_cast<void*>(TAlignedPagePool::POOL_PAGE_SIZE+1);
}

int TFakeUnalignedMmap::Munmap(void* addr, size_t size)
{
    if (OnMunmap) {
        OnMunmap(addr, size);
    }
    return 0;
}

TAlignedPagePoolCounters::TAlignedPagePoolCounters(::NMonitoring::TDynamicCounterPtr countersRoot, const TString& name) {
    if (!countersRoot || name.empty())
        return;
    ::NMonitoring::TDynamicCounterPtr subGroup = countersRoot->GetSubgroup("counters", "utils")->GetSubgroup("subsystem", "mkqlalloc");
    TotalBytesAllocatedCntr = subGroup->GetCounter(name + "/TotalBytesAllocated");
    AllocationsCntr = subGroup->GetCounter(name + "/Allocations", true);
    PoolsCntr = subGroup->GetCounter(name + "/Pools", true);
    LostPagesBytesFreeCntr = subGroup->GetCounter(name + "/LostPagesBytesFreed", true);
}

template<typename T>
TAlignedPagePoolImpl<T>::~TAlignedPagePoolImpl() {
    if (CheckLostMem && !UncaughtException()) {
        Y_DEBUG_ABORT_UNLESS(TotalAllocated == FreePages.size() * POOL_PAGE_SIZE,
                       "memory leak; Expected %ld, actual %ld (%ld page(s), %ld offloaded); allocator created at: %s",
                       TotalAllocated, FreePages.size() * POOL_PAGE_SIZE,
                       FreePages.size(), OffloadedActiveBytes, GetDebugInfo().data());
        Y_DEBUG_ABORT_UNLESS(OffloadedActiveBytes == 0, "offloaded: %ld", OffloadedActiveBytes);
    }

    size_t activeBlocksSize = 0;
    for (auto it = ActiveBlocks.cbegin(); ActiveBlocks.cend() != it; ActiveBlocks.erase(it++)) {
        activeBlocksSize += it->second;
#ifdef PROFILE_MEMORY_ALLOCATIONS
        ReturnBlock(it->first, it->second);
#else
        Free(it->first, it->second);
#endif
    }

    if (activeBlocksSize > 0 || FreePages.size() != AllPages.size() || OffloadedActiveBytes) {
        if (Counters.LostPagesBytesFreeCntr) {
            (*Counters.LostPagesBytesFreeCntr) += OffloadedActiveBytes + activeBlocksSize + (AllPages.size() - FreePages.size()) * POOL_PAGE_SIZE;
        }
    }

    Y_DEBUG_ABORT_UNLESS(TotalAllocated == AllPages.size() * POOL_PAGE_SIZE + OffloadedActiveBytes,
                   "Expected %ld, actual %ld (%ld page(s))", TotalAllocated,
                   AllPages.size() * POOL_PAGE_SIZE + OffloadedActiveBytes, AllPages.size());

    for (auto &ptr : AllPages) {
        TGlobalPools<T, false>::Instance().Get(0).PushPage(ptr);
    }

    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= TotalAllocated;
    }
    if (Counters.PoolsCntr) {
        --(*Counters.PoolsCntr);
    }
    TotalAllocated = 0;
}

template<typename T>
void TAlignedPagePoolImpl<T>::ReleaseFreePages() {
    TotalAllocated -= FreePages.size() * POOL_PAGE_SIZE;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= FreePages.size() * POOL_PAGE_SIZE;
    }

    for (; !FreePages.empty(); FreePages.pop()) {
        AllPages.erase(FreePages.top());
        TGlobalPools<T, false>::Instance().Get(0).PushPage(FreePages.top());
    }
}

template<typename T>
void TAlignedPagePoolImpl<T>::OffloadAlloc(ui64 size) {
    if (Limit && TotalAllocated + size > Limit && !TryIncreaseLimit(TotalAllocated + size)) {
        throw TMemoryLimitExceededException();
    }

    if (AllocNotifyCallback) {
        if (AllocNotifyCurrentBytes > AllocNotifyBytes) {
            AllocNotifyCallback();
            AllocNotifyCurrentBytes = 0;
        }
    }

    ++OffloadedAllocCount;
    OffloadedBytes += size;
    OffloadedActiveBytes += size;
    TotalAllocated += size;
    if (AllocNotifyCallback) {
        AllocNotifyCurrentBytes += size;
    }
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) += size;
    }

    if (Counters.AllocationsCntr) {
        ++(*Counters.AllocationsCntr);
    }

    UpdatePeaks();
}

template<typename T>
void TAlignedPagePoolImpl<T>::OffloadFree(ui64 size) noexcept {
    TotalAllocated -= size;
    OffloadedActiveBytes -= size;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= size;
    }
}

template<typename T>
void* TAlignedPagePoolImpl<T>::GetPage() {
    ++PageAllocCount;
    if (!FreePages.empty()) {
        ++PageHitCount;
        const auto res = FreePages.top();
        FreePages.pop();
        return res;
    }

    if (Limit && TotalAllocated + POOL_PAGE_SIZE > Limit && !TryIncreaseLimit(TotalAllocated + POOL_PAGE_SIZE)) {
        throw TMemoryLimitExceededException();
    }

    if (const auto ptr = TGlobalPools<T, false>::Instance().Get(0).GetPage()) {
        TotalAllocated += POOL_PAGE_SIZE;
        if (AllocNotifyCallback) {
            AllocNotifyCurrentBytes += POOL_PAGE_SIZE;
        }
        if (Counters.TotalBytesAllocatedCntr) {
            (*Counters.TotalBytesAllocatedCntr) += POOL_PAGE_SIZE;
        }
        ++PageGlobalHitCount;
        AllPages.emplace(ptr);

        UpdatePeaks();
        return ptr;
    }

    ++PageMissCount;

#ifdef PROFILE_MEMORY_ALLOCATIONS
    const auto res = GetBlock(POOL_PAGE_SIZE);
#else
    const auto res = Alloc(POOL_PAGE_SIZE);
#endif

    AllPages.emplace(res);
    return res;
}

template<typename T>
void TAlignedPagePoolImpl<T>::ReturnPage(void* addr) noexcept {
    Y_DEBUG_ABORT_UNLESS(AllPages.find(addr) != AllPages.end());
#ifdef PROFILE_MEMORY_ALLOCATIONS
    ReturnBlock(addr, POOL_PAGE_SIZE);
#else
    FreePages.emplace(addr);
#endif
}

template<typename T>
void* TAlignedPagePoolImpl<T>::GetBlock(size_t size) {
    Y_DEBUG_ABORT_UNLESS(size >= POOL_PAGE_SIZE);

#ifdef PROFILE_MEMORY_ALLOCATIONS
    OffloadAlloc(size);
    auto ret = malloc(size);
    if (!ret) {
        throw TMemoryLimitExceededException();
    }

    return ret;
#else
    if (size == POOL_PAGE_SIZE) {
        return GetPage();
    } else {
        const auto ptr = Alloc(size);
        Y_DEBUG_ABORT_UNLESS(ActiveBlocks.emplace(ptr, size).second);
        return ptr;
    }
#endif
}

template<typename T>
void TAlignedPagePoolImpl<T>::ReturnBlock(void* ptr, size_t size) noexcept {
    Y_DEBUG_ABORT_UNLESS(size >= POOL_PAGE_SIZE);

#ifdef PROFILE_MEMORY_ALLOCATIONS
    OffloadFree(size);
    free(ptr);
#else
    if (size == POOL_PAGE_SIZE) {
        ReturnPage(ptr);
    } else {
        Free(ptr, size);
        Y_DEBUG_ABORT_UNLESS(ActiveBlocks.erase(ptr));
    }
#endif
    UpdateMemoryYellowZone();
}

template<typename T>
void* TAlignedPagePoolImpl<T>::Alloc(size_t size) {
    void* res = nullptr;
    size = AlignUp(size, SYS_PAGE_SIZE);

    if (Limit && TotalAllocated + size > Limit && !TryIncreaseLimit(TotalAllocated + size)) {
        throw TMemoryLimitExceededException();
    }

    if (AllocNotifyCallback) {
        if (AllocNotifyCurrentBytes > AllocNotifyBytes) {
            AllocNotifyCallback();
            AllocNotifyCurrentBytes = 0;
        }
    }

    if (size > POOL_PAGE_SIZE && size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level >= 1 && level <= MidLevels);
        if (res = TGlobalPools<T, false>::Instance().Get(level).GetPage()) {
            TotalAllocated += size;
            if (AllocNotifyCallback) {
                AllocNotifyCurrentBytes += size;
            }
            if (Counters.TotalBytesAllocatedCntr) {
                (*Counters.TotalBytesAllocatedCntr) += size;
            }
            ++PageGlobalHitCount;
        } else {
            ++PageMissCount;
        }
    }

    if (!res) {
        auto allocSize = size + ALLOC_AHEAD_PAGES * POOL_PAGE_SIZE;
        void* mem = T::Mmap(allocSize);
        if (Y_UNLIKELY(MAP_FAILED == mem)) {
            ythrow yexception() << "Mmap failed to allocate " << (size + POOL_PAGE_SIZE) << " bytes: " << LastSystemErrorText();
        }

        res = AlignUp(mem, POOL_PAGE_SIZE);
        const size_t off = reinterpret_cast<intptr_t>(res) - reinterpret_cast<intptr_t>(mem);
        if (Y_UNLIKELY(off)) {
            // unmap prefix
            if (Y_UNLIKELY(0 != T::Munmap(mem, off))) {
                ythrow yexception() << "Munmap(0x"
                    << IntToString<16>(reinterpret_cast<uintptr_t>(mem))
                    << ", " << off << ") failed: " << LastSystemErrorText();
            }
        }
        // Extra space is also page-aligned. Put it to the free page list
        auto alignedSize = AlignUp(size, POOL_PAGE_SIZE);
        ui64 extraPages = (allocSize - off - alignedSize) / POOL_PAGE_SIZE;
        ui64 tail = (allocSize - off - alignedSize) % POOL_PAGE_SIZE;
        auto extraPage = reinterpret_cast<ui8*>(res) + alignedSize;
        for (ui64 i = 0; i < extraPages; ++i) {
            AllPages.emplace(extraPage);
            FreePages.emplace(extraPage);
            extraPage += POOL_PAGE_SIZE;
        }
        if (size != alignedSize) {
            // unmap unaligned hole
            if (Y_UNLIKELY(0 != T::Munmap(reinterpret_cast<ui8*>(res) + size, alignedSize - size))) {
                ythrow yexception() << "Munmap(0x"
                    << IntToString<16>(reinterpret_cast<uintptr_t>(reinterpret_cast<ui8*>(res)+size))
                    << ", " << alignedSize - size
                    << ") failed: " << LastSystemErrorText();
            }
        }
        if (tail) {
            // unmap suffix
            Y_DEBUG_ABORT_UNLESS(extraPage+tail <= reinterpret_cast<ui8*>(mem) + size + ALLOC_AHEAD_PAGES * POOL_PAGE_SIZE);
            if (Y_UNLIKELY(0 != T::Munmap(extraPage, tail))) {
                ythrow yexception() << "Munmap(0x"
                    << IntToString<16>(reinterpret_cast<uintptr_t>(extraPage))
                    << ", " << tail
                    << ") failed: " << LastSystemErrorText();
            }
        }
        auto extraSize = extraPages * POOL_PAGE_SIZE;
        auto totalSize = size + extraSize;
        TotalAllocated += totalSize;
        if (AllocNotifyCallback) {
            AllocNotifyCurrentBytes += totalSize;
        }
        if (Counters.TotalBytesAllocatedCntr) {
            (*Counters.TotalBytesAllocatedCntr) += totalSize;
        }
    }

    if (Counters.AllocationsCntr) {
        ++(*Counters.AllocationsCntr);
    }
    ++AllocCount;
    UpdatePeaks();
    return res;
}

template<typename T>
void TAlignedPagePoolImpl<T>::Free(void* ptr, size_t size) noexcept {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size <= MaxMidSize)
        size = FastClp2(size);
    if (size <= MaxMidSize) {
        auto level = LeastSignificantBit(size) - LeastSignificantBit(POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level >= 1 && level <= MidLevels);
        TGlobalPools<T, false>::Instance().Get(level).PushPage(ptr);
    } else {
        Y_ABORT_UNLESS(!T::Munmap(ptr, size));
    }

    Y_DEBUG_ABORT_UNLESS(TotalAllocated >= size);
    TotalAllocated -= size;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= size;
    }
}

template<typename T>
void TAlignedPagePoolImpl<T>::UpdateMemoryYellowZone() {
    if (Limit == 0) return;
    if (IsMemoryYellowZoneForcefullyChanged) return;
    // if (IncreaseMemoryLimitCallback && !IsMaximumLimitValueReached) return;

    ui8 usedMemoryPercent = 100 * GetUsed() / Limit;
    if (usedMemoryPercent >= EnableMemoryYellowZoneThreshold) {
        IsMemoryYellowZoneReached = true;
    } else if (usedMemoryPercent <= DisableMemoryYellowZoneThreshold) {
        IsMemoryYellowZoneReached = false;
    }
}

template<typename T>
bool TAlignedPagePoolImpl<T>::TryIncreaseLimit(ui64 required) {
    if (!IncreaseMemoryLimitCallback) {
        return false;
    }
    IncreaseMemoryLimitCallback(Limit, required);
    return Limit >= required;
}

template<typename T>
ui64 TAlignedPagePoolImpl<T>::GetGlobalPagePoolSize() {
    ui64 size = 0;
    for (size_t level = 0; level <= MidLevels; ++level) {
        size += TGlobalPools<T, false>::Instance().Get(level).GetSize();
    }
    return size;
}

template<typename T>
void TAlignedPagePoolImpl<T>::PrintStat(size_t usedPages, IOutputStream& out) const {
    usedPages += GetFreePageCount();
    out << "Count of free pages: " << GetFreePageCount() << Endl;
    out << "Allocated for blocks: " << (GetAllocated() - usedPages * POOL_PAGE_SIZE) << Endl;
    out << "Total allocated by lists: " << GetAllocated() << Endl;
}

template<typename T>
void TAlignedPagePoolImpl<T>::ResetGlobalsUT()
{
    TGlobalPools<T, false>::Instance().Reset();
}

template class TAlignedPagePoolImpl<>;
template class TAlignedPagePoolImpl<TFakeAlignedMmap>;
template class TAlignedPagePoolImpl<TFakeUnalignedMmap>;

template<typename TMmap>
void* GetAlignedPage(ui64 size) {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size < TAlignedPagePool::POOL_PAGE_SIZE) {
        size = TAlignedPagePool::POOL_PAGE_SIZE;
    }

    if (size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        if (auto res = TGlobalPools<TMmap, true>::Instance().Get(level).GetPage()) {
            return res;
        }
    }

    auto allocSize = Max<ui64>(MaxMidSize, size);
    void* mem = TMmap::Mmap(allocSize);
    if (Y_UNLIKELY(MAP_FAILED == mem)) {
        ythrow yexception() << "Mmap failed to allocate " << allocSize << " bytes: " << LastSystemErrorText();
    }

    if (size < MaxMidSize) {
        // push extra allocated pages to cache
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        auto& pool = TGlobalPools<TMmap, true>::Instance().Get(level);
        ui8* ptr = (ui8*)mem + size;
        ui8* const end = (ui8*)mem + MaxMidSize;
        while (ptr < end) {
            pool.PushPage(ptr);
            ptr += size;
        }
    }

    return mem;
}

template<typename TMmap>
void ReleaseAlignedPage(void* mem, ui64 size) {
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size < TAlignedPagePool::POOL_PAGE_SIZE) {
        size = TAlignedPagePool::POOL_PAGE_SIZE;
    }

    if (size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_DEBUG_ABORT_UNLESS(level <= MidLevels);
        TGlobalPools<TMmap, true>::Instance().Get(level).PushPage(mem);
        return;
    }

    if (Y_UNLIKELY(0 != TMmap::Munmap(mem, size))) {
        ythrow yexception() << "Munmap(0x"
            << IntToString<16>(reinterpret_cast<uintptr_t>(mem))
            << ", " << size << ") failed: " << LastSystemErrorText();
    }
}

template void* GetAlignedPage<>(ui64);
template void* GetAlignedPage<TFakeAlignedMmap>(ui64);
template void* GetAlignedPage<TFakeUnalignedMmap>(ui64);

template void ReleaseAlignedPage<>(void*,ui64);
template void ReleaseAlignedPage<TFakeAlignedMmap>(void*,ui64);
template void ReleaseAlignedPage<TFakeUnalignedMmap>(void*,ui64);

} // NKikimr
