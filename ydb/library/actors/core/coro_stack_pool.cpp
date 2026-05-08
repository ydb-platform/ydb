#include "coro_stack_pool.h"

#include <util/generic/yexception.h>
#include <util/system/context.h>
#include <util/system/info.h>
#include <util/system/protect.h>
#include <util/system/yassert.h>

#include <array>
#include <atomic>
#include <algorithm>
#include <bit>
#include <mutex>
#include <vector>

#if defined(_unix_) || defined(_darwin_)
#include <sys/mman.h>
#elif defined(_win_)
#include <Windows.h>
#else
#error TStackMemPool requires mmap or VirtualAlloc
#endif

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace NActors {

static const size_t PageSize = NSystemInfo::GetPageSize();
static constexpr ui32 MaxStackPageBits = 20;
static constexpr size_t LocalCacheLimitBytes = 1 * 1024 * 1024;
static constexpr size_t GlobalExchangeLimitBytes = 64 * 1024 * 1024;

// The stack memory layout and mprotect test cover only downward-growing stacks.
// Revisit guard placement before enabling upward-growing stacks.
static_assert(STACK_GROW_DOWN == 1, "TStackMemPool requires downward-growing stacks");

static struct {
    std::mutex Mutex;
    std::array<std::atomic<TStackMemPool*>, MaxStackPageBits> Pools;
} StackMemPoolRegistry;

static size_t GetMappingSize(TStackMemPool::TPageNumber pageNumber) noexcept {
    return pageNumber.Size() + PageSize;
}

static size_t GetCacheLimit(size_t limitBytes, TStackMemPool::TPageNumber pageNumber) noexcept {
    return std::max<size_t>(1, limitBytes / GetMappingSize(pageNumber));
}

static char* AllocateStackMemMapping(size_t mappingSize) {
#if defined(_unix_) || defined(_darwin_)
    char* mapping = static_cast<char*>(mmap(nullptr, mappingSize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) {
        ythrow TSystemError() << "mmap failed for coroutine stack memory";
    }
    return mapping;
#elif defined(_win_)
    char* mapping = static_cast<char*>(VirtualAlloc(nullptr, mappingSize, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE));
    if (!mapping) {
        ythrow TSystemError() << "VirtualAlloc failed for coroutine stack memory";
    }
    return mapping;
#endif
}

static void FreeStackMemMapping(char* mapping, size_t mappingSize) noexcept {
#if defined(_unix_) || defined(_darwin_)
    Y_ABORT_UNLESS(munmap(mapping, mappingSize) == 0);
#elif defined(_win_)
    (void)mappingSize;
    Y_ABORT_UNLESS(VirtualFree(mapping, 0, MEM_RELEASE));
#endif
}

static char* GetStackMemMappingBegin(char* stackMem) noexcept {
    return stackMem - PageSize;
}

static char* AllocateStackMem(TStackMemPool::TPageNumber pageNumber) {
    const size_t mappingSize = GetMappingSize(pageNumber);
    char* mapping = AllocateStackMemMapping(mappingSize);
    char* stackMem = mapping + PageSize;
    try {
        ProtectMemory(mapping, PageSize, EProtectMemoryMode::PM_NONE);
    } catch (...) {
        FreeStackMemMapping(mapping, mappingSize);
        throw;
    }

    return stackMem;
}

static void FreeStackMem(TStackMemPool::TPageNumber pageNumber, char* stackMem) noexcept {
    if (!stackMem) {
        return;
    }

    FreeStackMemMapping(GetStackMemMappingBegin(stackMem), GetMappingSize(pageNumber));
}

struct TStackMemChain {
    char* TryPop() noexcept {
        if (StackMems.empty()) {
            return nullptr;
        }

        char* stackMem = StackMems.back();
        StackMems.pop_back();
        return stackMem;
    }

    bool Push(char* stackMem, size_t limit) noexcept {
        if (StackMems.size() >= limit) {
            return false;
        }

        StackMems.push_back(stackMem);
        return true;
    }

    void Clear(TStackMemPool::TPageNumber pageNumber) noexcept {
        for (char* stackMem : StackMems) {
            FreeStackMem(pageNumber, stackMem);
        }
        StackMems.clear();
    }

    std::vector<char*> StackMems;
};

struct TGlobalStackExchange {
    struct TLockedChain {
        char* TryPop() noexcept {
            std::lock_guard<std::mutex> guard(Mutex);
            return Chain.TryPop();
        }

        bool Push(char* stackMem, size_t limit) noexcept {
            std::lock_guard<std::mutex> guard(Mutex);
            return Chain.Push(stackMem, limit);
        }

        void Clear(TStackMemPool::TPageNumber pageNumber) noexcept {
            std::lock_guard<std::mutex> guard(Mutex);
            Chain.Clear(pageNumber);
        }

        TStackMemChain Chain;
        std::mutex Mutex;
    };

    ~TGlobalStackExchange() {
        for (size_t i = 0; i < Chains.size(); ++i) {
            Chains[i].Clear(TStackMemPool::TPageNumber{static_cast<ui32>(i)});
        }
    }

    char* Allocate(TStackMemPool::TPageNumber pageNumber) noexcept {
        return Chains[pageNumber.NumPages].TryPop();
    }

    bool Put(TStackMemPool::TPageNumber pageNumber, char* stackMem) noexcept {
        return Chains[pageNumber.NumPages].Push(stackMem, GetCacheLimit(GlobalExchangeLimitBytes, pageNumber));
    }

    std::array<TLockedChain, MaxStackPageBits> Chains;
};

static TGlobalStackExchange GlobalExchange;

struct TStackMemPoolCache {
    ~TStackMemPoolCache() {
        for (size_t i = 0; i < Chains.size(); ++i) {
            Chains[i].Clear(TStackMemPool::TPageNumber{static_cast<ui32>(i)});
        }
    }

    char* Allocate(TStackMemPool::TPageNumber pageNumber) noexcept {
        if (char* stackMem = Chains[pageNumber.NumPages].TryPop()) {
            return stackMem;
        }

        return GlobalExchange.Allocate(pageNumber);
    }

    void Put(TStackMemPool::TPageNumber pageNumber, char* stackMem) noexcept {
        if (!Chains[pageNumber.NumPages].Push(stackMem, GetCacheLimit(LocalCacheLimitBytes, pageNumber))) {
            if (!GlobalExchange.Put(pageNumber, stackMem)) {
                FreeStackMem(pageNumber, stackMem);
            }
        }
    }

    void Free(TStackMemPool::TPageNumber pageNumber, char* stackMem) noexcept {
        if (!stackMem) {
            return;
        }

        Put(pageNumber, stackMem);
    }

    std::array<TStackMemChain, MaxStackPageBits> Chains;
};

static thread_local TStackMemPoolCache LocalCache;

class TStackMem final : public IStackMem {
public:
    TStackMem(char* stackMem, TStackMemPool::TPageNumber pageNumber) noexcept
        : StackMem(stackMem)
        , PageNumber(pageNumber)
    {}

    ~TStackMem() override {
        LocalCache.Free(PageNumber, StackMem);
    }

    char* Begin() noexcept override {
        return StackMem;
    }

    char* End() noexcept override {
        return StackMem + PageNumber.Size();
    }

private:
    char* StackMem;
    const TStackMemPool::TPageNumber PageNumber;
};

TStackMemPool::TPageNumber TStackMemPool::TPageNumber::Bytes(ui32 sz) {
    if (sz == 0 || (sz & (sz - 1u))) {
        ythrow yexception() << "The stack size must be power of 2";
    }

    const ui32 pages = (sz + PageSize - 1) / PageSize;
    const ui32 numPages = std::bit_width(std::bit_ceil(pages)) - 1;

    if (numPages >= MaxStackPageBits) {
        ythrow yexception() << "The stack requires too many pages: " << numPages;
    }

    return TPageNumber{numPages};
}

size_t TStackMemPool::TPageNumber::Size() const noexcept {
    return PageSize << NumPages;
}


TStackMemPool* TStackMemPool::GetMemPool(TPageNumber pn) noexcept {
    Y_ABORT_UNLESS(pn.NumPages < MaxStackPageBits);
    TStackMemPool* pool = StackMemPoolRegistry.Pools[pn.NumPages].load(std::memory_order_acquire);
    if (pool) {
        return pool;
    }

    std::lock_guard<std::mutex> guard(StackMemPoolRegistry.Mutex);
    pool = StackMemPoolRegistry.Pools[pn.NumPages].load(std::memory_order_relaxed);
    if (!pool) {
        pool = new TStackMemPool(pn);
        StackMemPoolRegistry.Pools[pn.NumPages].store(pool, std::memory_order_release);
    }

    return pool;
}

TStackMemPool::TStackMemPool(TPageNumber pageNumber) noexcept
    : PageNumber(pageNumber)
{}

std::unique_ptr<IStackMem> TStackMemPool::Allocate() {
    char* stackMem = LocalCache.Allocate(PageNumber);
    if (!stackMem) {
        stackMem = AllocateStackMem(PageNumber);
    }

    return std::unique_ptr<IStackMem>(new TStackMem(stackMem, PageNumber));
}

}
