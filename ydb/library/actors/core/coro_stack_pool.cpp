#include "coro_stack_pool.h"

#include <util/generic/yexception.h>
#include <util/system/context.h>
#include <util/system/info.h>
#include <util/system/yassert.h>

#include <library/cpp/threading/queue/mpmc_unordered_ring.h>

#include <array>
#include <atomic>
#include <algorithm>
#include <bit>
#include <cerrno>
#include <cstring>
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

static size_t GetMappingSize(TStackMemPool::TPageBucket pageBucket) noexcept {
    return pageBucket.Size() + PageSize;
}

static size_t GetCacheLimit(size_t limitBytes, TStackMemPool::TPageBucket pageBucket) noexcept {
    // TODO: Precompute limits per page bucket to avoid division in the local cache put path.
    return std::max<size_t>(1, limitBytes / GetMappingSize(pageBucket));
}

static char* AllocateStackMemMapping(size_t mappingSize) {
#if defined(_unix_) || defined(_darwin_)
    char* mapping = static_cast<char*>(mmap(nullptr, mappingSize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    Y_ABORT_UNLESS(mapping != MAP_FAILED, "mmap failed for coroutine stack memory, mappingSize# %zu errno# %d (%s)",
        mappingSize, errno, std::strerror(errno));
    return mapping;
#elif defined(_win_)
    char* mapping = static_cast<char*>(VirtualAlloc(nullptr, mappingSize, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE));
    Y_ABORT_UNLESS(mapping, "VirtualAlloc failed for coroutine stack memory, mappingSize# %zu lastError# %lu",
        mappingSize, GetLastError());
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

static void ProtectStackMemGuard(char* mapping) noexcept {
#if defined(_unix_) || defined(_darwin_)
    Y_ABORT_UNLESS(mprotect(mapping, PageSize, PROT_NONE) == 0,
        "mprotect failed for coroutine stack guard page, errno# %d (%s)", errno, std::strerror(errno));
#elif defined(_win_)
    DWORD oldProtect = 0;
    Y_ABORT_UNLESS(VirtualProtect(mapping, PageSize, PAGE_NOACCESS, &oldProtect),
        "VirtualProtect failed for coroutine stack guard page, lastError# %lu", GetLastError());
#endif
}

static char* AllocateStackMem(TStackMemPool::TPageBucket pageBucket) {
    const size_t mappingSize = GetMappingSize(pageBucket);
    char* mapping = AllocateStackMemMapping(mappingSize);
    char* stackMem = mapping + PageSize;
    ProtectStackMemGuard(mapping);

    return stackMem;
}

static void FreeStackMem(TStackMemPool::TPageBucket pageBucket, char* stackMem) noexcept {
    if (!stackMem) {
        return;
    }

    FreeStackMemMapping(GetStackMemMappingBegin(stackMem), GetMappingSize(pageBucket));
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

    void Clear(TStackMemPool::TPageBucket pageBucket) noexcept {
        for (char* stackMem : StackMems) {
            FreeStackMem(pageBucket, stackMem);
        }
        StackMems.clear();
    }

    std::vector<char*> StackMems;
};

struct TGlobalStackExchange {
    struct TLockFreeChain {
        explicit TLockFreeChain(size_t limit)
            : StackMems(limit)
        {}

        char* TryPop() noexcept {
            return static_cast<char*>(StackMems.Pop());
        }

        bool Push(char* stackMem) noexcept {
            return StackMems.Push(stackMem);
        }

        void Clear(TStackMemPool::TPageBucket pageBucket) noexcept {
            while (char* stackMem = TryPop()) {
                FreeStackMem(pageBucket, stackMem);
            }

            ui64 last = 0;
            while (char* stackMem = static_cast<char*>(StackMems.UnsafeScanningPop(&last))) {
                FreeStackMem(pageBucket, stackMem);
            }
        }

        NThreading::TMPMCUnorderedRing StackMems;
    };

    TGlobalStackExchange() {
        for (size_t i = 0; i < Chains.size(); ++i) {
            Chains[i] = std::make_unique<TLockFreeChain>(
                GetCacheLimit(GlobalExchangeLimitBytes, TStackMemPool::TPageBucket{static_cast<ui32>(i)}));
        }
    }

    ~TGlobalStackExchange() {
        for (size_t i = 0; i < Chains.size(); ++i) {
            Chains[i]->Clear(TStackMemPool::TPageBucket{static_cast<ui32>(i)});
        }
    }

    char* Allocate(TStackMemPool::TPageBucket pageBucket) noexcept {
        return Chains[pageBucket.Index]->TryPop();
    }

    bool Put(TStackMemPool::TPageBucket pageBucket, char* stackMem) noexcept {
        return Chains[pageBucket.Index]->Push(stackMem);
    }

    std::array<std::unique_ptr<TLockFreeChain>, MaxStackPageBits> Chains;
};

static TGlobalStackExchange GlobalExchange;

struct TStackMemPoolCache {
    ~TStackMemPoolCache() {
        for (size_t i = 0; i < Chains.size(); ++i) {
            Chains[i].Clear(TStackMemPool::TPageBucket{static_cast<ui32>(i)});
        }
    }

    char* Allocate(TStackMemPool::TPageBucket pageBucket) noexcept {
        if (char* stackMem = Chains[pageBucket.Index].TryPop()) {
            return stackMem;
        }

        return GlobalExchange.Allocate(pageBucket);
    }

    void Put(TStackMemPool::TPageBucket pageBucket, char* stackMem) noexcept {
        if (!Chains[pageBucket.Index].Push(stackMem, GetCacheLimit(LocalCacheLimitBytes, pageBucket))) {
            if (!GlobalExchange.Put(pageBucket, stackMem)) {
                FreeStackMem(pageBucket, stackMem);
            }
        }
    }

    void Free(TStackMemPool::TPageBucket pageBucket, char* stackMem) noexcept {
        if (!stackMem) {
            return;
        }

        Put(pageBucket, stackMem);
    }

    std::array<TStackMemChain, MaxStackPageBits> Chains;
};

static thread_local TStackMemPoolCache LocalCache;

class TStackMem final : public IStackMem {
public:
    TStackMem(char* stackMem, TStackMemPool::TPageBucket pageBucket) noexcept
        : StackMem(stackMem)
        , PageBucket(pageBucket)
    {}

    ~TStackMem() override {
        LocalCache.Free(PageBucket, StackMem);
    }

    char* Begin() noexcept override {
        return StackMem;
    }

    char* End() noexcept override {
        return StackMem + PageBucket.Size();
    }

private:
    char* StackMem;
    const TStackMemPool::TPageBucket PageBucket;
};

TStackMemPool::TPageBucket TStackMemPool::TPageBucket::Bytes(ui32 sz) {
    if (sz == 0 || (sz & (sz - 1u))) {
        ythrow yexception() << "The stack size must be power of 2";
    }

    const ui32 pages = (sz + PageSize - 1) / PageSize;
    const ui32 bucketIndex = std::bit_width(pages - 1);

    if (bucketIndex >= MaxStackPageBits) {
        ythrow yexception() << "The stack requires too large page bucket: " << bucketIndex;
    }

    return TPageBucket{bucketIndex};
}

size_t TStackMemPool::TPageBucket::Size() const noexcept {
    return PageSize << Index;
}


TStackMemPool* TStackMemPool::GetMemPool(TPageBucket pageBucket) noexcept {
    Y_ABORT_UNLESS(pageBucket.Index < MaxStackPageBits);
    TStackMemPool* pool = StackMemPoolRegistry.Pools[pageBucket.Index].load(std::memory_order_acquire);
    if (pool) {
        return pool;
    }

    std::lock_guard<std::mutex> guard(StackMemPoolRegistry.Mutex);
    pool = StackMemPoolRegistry.Pools[pageBucket.Index].load(std::memory_order_relaxed);
    if (!pool) {
        pool = new TStackMemPool(pageBucket);
        StackMemPoolRegistry.Pools[pageBucket.Index].store(pool, std::memory_order_release);
    }

    return pool;
}

TStackMemPool::TStackMemPool(TPageBucket pageBucket) noexcept
    : PageBucket(pageBucket)
{}

std::unique_ptr<IStackMem> TStackMemPool::Allocate() {
    char* stackMem = LocalCache.Allocate(PageBucket);
    if (!stackMem) {
        stackMem = AllocateStackMem(PageBucket);
    }

    return std::unique_ptr<IStackMem>(new TStackMem(stackMem, PageBucket));
}

}
