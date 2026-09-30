#pragma once

#include "aligned_page_pool.h"

#include <util/system/compiler.h>
#include <util/system/error.h>
#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/thread/lfstack.h>

#include <yql/essentials/public/udf/sanitizer_utils.h>

#include <atomic>
#include <cstddef>

namespace NKikimr {

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
        Y_DEBUG_ABORT_UNLESS(0 == res, "Madvise failed: %s", LastSystemErrorText());
    }

    T& Provider_;
    const size_t PageSize_;
    std::atomic<ui64> Count_ = 0;
    TLockFreeStack<void*> Pages_;
};

} // namespace NKikimr
