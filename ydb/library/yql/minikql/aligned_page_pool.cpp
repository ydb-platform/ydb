#include "aligned_page_pool.h"
#include <library/cpp/actors/util/intrinsics.h>

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

class TGlobalPagePool {
public:
    TGlobalPagePool(size_t pageSize)
        : PageSize(pageSize)
    {}

    ~TGlobalPagePool() {
        void* addr = nullptr;
        while (Pages.Dequeue(&addr)) {
#ifdef _win_
            Y_VERIFY_DEBUG(::VirtualFree(addr, 0, MEM_RELEASE), "VirtualFree failed: %s", LastSystemErrorText());
#else
            Y_VERIFY_DEBUG(0 == ::munmap(addr, PageSize), "munmap failed: %s", LastSystemErrorText());
#endif
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
        AtomicIncrement(Count);
        Pages.Enqueue(addr);
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
    const size_t PageSize;
    TAtomic Count = 0;
    TLockFreeStack<void*> Pages;
};

class TGlobalPools {
public:
    static TGlobalPools& Instance() {
        return *Singleton<TGlobalPools>();
    }

    TGlobalPagePool& Get(ui32 index) {
        return *Pools[index];
    }

    TGlobalPools()
    {
        Pools.reserve(MidLevels + 1);
        for (ui32 i = 0; i <= MidLevels; ++i) {
            Pools.emplace_back(MakeHolder<TGlobalPagePool>(TAlignedPagePool::POOL_PAGE_SIZE << i));
        }
    }

private:
    TVector<THolder<TGlobalPagePool>> Pools;
};

} // unnamed

TAlignedPagePoolCounters::TAlignedPagePoolCounters(NMonitoring::TDynamicCounterPtr countersRoot, const TString& name) {
    if (!countersRoot || name.empty())
        return;
    NMonitoring::TDynamicCounterPtr subGroup = countersRoot->GetSubgroup("counters", "utils")->GetSubgroup("subsystem", "mkqlalloc");
    TotalBytesAllocatedCntr = subGroup->GetCounter(name + "/TotalBytesAllocated");
    AllocationsCntr = subGroup->GetCounter(name + "/Allocations", true);
    PoolsCntr = subGroup->GetCounter(name + "/Pools", true);
    LostPagesBytesFreeCntr = subGroup->GetCounter(name + "/LostPagesBytesFreed", true);
}

TAlignedPagePool::~TAlignedPagePool() {
    if (CheckLostMem && !UncaughtException()) {
        Y_VERIFY_DEBUG(TotalAllocated == FreePages.size() * POOL_PAGE_SIZE,
                       "Expected %ld, actual %ld (%ld page(s), %ld offloaded)", TotalAllocated,
                       FreePages.size() * POOL_PAGE_SIZE, FreePages.size(), OffloadedActiveBytes);
        Y_VERIFY_DEBUG(OffloadedActiveBytes == 0, "offloaded: %ld", OffloadedActiveBytes);
    }

    size_t activeBlocksSize = 0;
    for (auto it = ActiveBlocks.cbegin(); ActiveBlocks.cend() != it; ActiveBlocks.erase(it++)) {
        activeBlocksSize += it->second;
        Free(it->first, it->second);
    } 
 
    if (activeBlocksSize > 0 || FreePages.size() != AllPages.size() || OffloadedActiveBytes) {
        if (Counters.LostPagesBytesFreeCntr) {
            (*Counters.LostPagesBytesFreeCntr) += OffloadedActiveBytes + activeBlocksSize + (AllPages.size() - FreePages.size()) * POOL_PAGE_SIZE;
        }
    }

    Y_VERIFY_DEBUG(TotalAllocated == AllPages.size() * POOL_PAGE_SIZE + OffloadedActiveBytes,
                   "Expected %ld, actual %ld (%ld page(s))", TotalAllocated,
                   AllPages.size() * POOL_PAGE_SIZE + OffloadedActiveBytes, AllPages.size());

    for (auto &ptr : AllPages) {
        TGlobalPools::Instance().Get(0).PushPage(ptr);
    }

    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= TotalAllocated;
    }
    if (Counters.PoolsCntr) {
        --(*Counters.PoolsCntr);
    }
    TotalAllocated = 0;
}
 
void TAlignedPagePool::ReleaseFreePages() {
    TotalAllocated -= FreePages.size() * POOL_PAGE_SIZE;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= FreePages.size() * POOL_PAGE_SIZE;
    }

    for (; !FreePages.empty(); FreePages.pop()) { 
        AllPages.erase(FreePages.top());
        TGlobalPools::Instance().Get(0).PushPage(FreePages.top());
    }
}

void TAlignedPagePool::OffloadAlloc(ui64 size) {
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

void TAlignedPagePool::OffloadFree(ui64 size) noexcept { 
    TotalAllocated -= size;
    OffloadedActiveBytes -= size;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= size;
    }
}

void* TAlignedPagePool::GetPage() {
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

    if (const auto ptr = TGlobalPools::Instance().Get(0).GetPage()) {
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
    const auto res = Alloc(POOL_PAGE_SIZE); 
    AllPages.emplace(res); 
    return res; 
}

void TAlignedPagePool::ReturnPage(void* addr) noexcept { 
    Y_VERIFY_DEBUG(AllPages.find(addr) != AllPages.end());
    FreePages.emplace(addr); 
} 
 
void* TAlignedPagePool::GetBlock(size_t size) { 
    Y_VERIFY_DEBUG(size >= POOL_PAGE_SIZE);
    if (size == POOL_PAGE_SIZE) { 
        return GetPage(); 
    } else { 
        const auto ptr = Alloc(size); 
        Y_VERIFY_DEBUG(ActiveBlocks.emplace(ptr, size).second); 
        return ptr; 
    } 
} 
 
void TAlignedPagePool::ReturnBlock(void* ptr, size_t size) noexcept { 
    Y_VERIFY_DEBUG(size >= POOL_PAGE_SIZE);
    if (size == POOL_PAGE_SIZE) { 
        ReturnPage(ptr); 
    } else { 
        Free(ptr, size); 
        Y_VERIFY_DEBUG(ActiveBlocks.erase(ptr)); 
    } 
} 
 
void* TAlignedPagePool::Alloc(size_t size) {
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

    if (size > TAlignedPagePool::POOL_PAGE_SIZE && size <= MaxMidSize) {
        size = FastClp2(size);
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_VERIFY_DEBUG(level >= 1 && level <= MidLevels);
        if (res = TGlobalPools::Instance().Get(level).GetPage()) {
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
#ifdef _win_
        res = ::VirtualAlloc(0, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
        if (Y_UNLIKELY(0 == res)) {
            ythrow yexception() << "VirtualAlloc failed to allocate " << size << " bytes: " << LastSystemErrorText();
        }
        Y_VERIFY((reinterpret_cast<uintptr_t>(res) & PAGE_ADDR_MASK) == reinterpret_cast<uintptr_t>(res), "Got unaligned address");
        TotalAllocated += size;
        if (AllocNotifyCallback) {
            AllocNotifyCurrentBytes += size;
        }
        if (Counters.TotalBytesAllocatedCntr) {
            (*Counters.TotalBytesAllocatedCntr) += size;
        }
#else
        void* mem = ::mmap(nullptr, size + POOL_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, 0, 0);
        if (Y_UNLIKELY(MAP_FAILED == mem)) {
            ythrow yexception() << "mmap failed to allocate " << (size + POOL_PAGE_SIZE) << " bytes: " << LastSystemErrorText();
        }

        if ((reinterpret_cast<uintptr_t>(mem) & PAGE_ADDR_MASK) == reinterpret_cast<uintptr_t>(mem)) {
            // We got already aligned address
            res = mem;
            if (AlignUp(size, POOL_PAGE_SIZE) == size) {
                // Extra space is also page-aligned. Put it to the free page list
                const auto extraPage = reinterpret_cast<ui8*>(mem) + size;
                AllPages.emplace(extraPage);
                FreePages.emplace(extraPage);
                TotalAllocated += size + POOL_PAGE_SIZE;
                if (AllocNotifyCallback) {
                    AllocNotifyCurrentBytes += size + POOL_PAGE_SIZE;
                }
                if (Counters.TotalBytesAllocatedCntr) {
                    (*Counters.TotalBytesAllocatedCntr) += size + POOL_PAGE_SIZE;
                }
            } else {
                // Return extra space to the system
                if (Y_UNLIKELY(0 != ::munmap(reinterpret_cast<ui8*>(mem) + size, POOL_PAGE_SIZE))) {
                    ythrow yexception() << "munmap(0x" << IntToString<16>(reinterpret_cast<uintptr_t>(mem) +  size)
                        << ", " << (0 + POOL_PAGE_SIZE) << ") failed: " << LastSystemErrorText();
                }
                TotalAllocated += size;
                if (AllocNotifyCallback) {
                    AllocNotifyCurrentBytes += size;
                }
                if (Counters.TotalBytesAllocatedCntr) {
                    (*Counters.TotalBytesAllocatedCntr) += size;
                }
            }
        } else {
            res = reinterpret_cast<void*>((reinterpret_cast<uintptr_t>(mem) & PAGE_ADDR_MASK) + POOL_PAGE_SIZE);
            const size_t off = reinterpret_cast<intptr_t>(res) - reinterpret_cast<intptr_t>(mem);
            // Return extra space before/after aligned region to the system
            if (Y_UNLIKELY(0 != ::munmap(mem, off))) {
                ythrow yexception() << "munmap(0x" << IntToString<16>(reinterpret_cast<uintptr_t>(mem)) << ", " << off
                    << ") failed: " << LastSystemErrorText();
            }
            if (Y_UNLIKELY(0 != ::munmap(reinterpret_cast<ui8*>(res) + size, POOL_PAGE_SIZE - off))) {
                ythrow yexception() << "munmap(0x" << IntToString<16>(reinterpret_cast<uintptr_t>(res) +  size)
                    << ", " << (POOL_PAGE_SIZE - off) << ") failed: " << LastSystemErrorText();
            }
            TotalAllocated += size;
            if (AllocNotifyCallback) {
                AllocNotifyCurrentBytes += size;
            }
            if (Counters.TotalBytesAllocatedCntr) {
                (*Counters.TotalBytesAllocatedCntr) += size;
            }
        }
#endif
    }

    if (Counters.AllocationsCntr) {
        ++(*Counters.AllocationsCntr);
    }
    ++AllocCount;
    UpdatePeaks();
    return res;
}

void TAlignedPagePool::Free(void* ptr, size_t size) noexcept { 
    size = AlignUp(size, SYS_PAGE_SIZE);
    if (size <= MaxMidSize)
        size = FastClp2(size);
    if (size <= MaxMidSize) {
        auto level = LeastSignificantBit(size) - LeastSignificantBit(TAlignedPagePool::POOL_PAGE_SIZE);
        Y_VERIFY_DEBUG(level >= 1 && level <= MidLevels);
        TGlobalPools::Instance().Get(level).PushPage(ptr);
    } else {
#ifdef _win_
        Y_VERIFY(::VirtualFree(ptr, 0, MEM_RELEASE)); 
#else
        Y_VERIFY(!::munmap(ptr, size)); 
#endif
    }

    Y_VERIFY_DEBUG(TotalAllocated >= size);
    TotalAllocated -= size;
    if (Counters.TotalBytesAllocatedCntr) {
        (*Counters.TotalBytesAllocatedCntr) -= size;
    }
}

bool TAlignedPagePool::TryIncreaseLimit(ui64 required) {
    if (!IncreaseMemoryLimitCallback) {
        return false;
    }
    IncreaseMemoryLimitCallback(Limit, required);
    return Limit >= required;
}

ui64 TAlignedPagePool::GetGlobalPagePoolSize() {
    ui64 size = 0;
    for (size_t level = 0; level <= MidLevels; ++level) {
        size += TGlobalPools::Instance().Get(level).GetSize();
    }
    return size;
}

void TAlignedPagePool::PrintStat(size_t usedPages, IOutputStream& out) const {
    usedPages += GetFreePageCount();
    out << "Count of free pages: " << GetFreePageCount() << Endl;
    out << "Allocated for blocks: " << (GetAllocated() - usedPages * TAlignedPagePool::POOL_PAGE_SIZE) << Endl;
    out << "Total allocated by lists: " << GetAllocated() << Endl;
}

} // NKikimr
