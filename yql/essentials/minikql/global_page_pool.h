#pragma once

#include "aligned_page_pool.h"
#include "frozen_page.h"

#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/thread/lfstack.h>

#include <yql/essentials/public/udf/sanitizer_utils/sanitizer_utils.h>

#include <atomic>
#include <cstddef>
#include <utility>

namespace NKikimr {

template <typename T, bool SysAlign>
class TGlobalPagePool {
public:
    TGlobalPagePool(T& provider, size_t pageSize)
        : Provider_(provider)
        , PageSize_(pageSize)
    {
    }

    ~TGlobalPagePool() {
        void* addr = nullptr;
        while (Pages_.Dequeue(&addr)) {
            UnmapPage(addr);
        }
    }

    void* GetPage() {
        if (void* page = GetCachedPage()) {
            return page;
        }

        return GetDecommittedPage();
    }

    void PushPage(void* addr) {
        NYql::NUdf::SanitizerMakeRegionInaccessible(addr, PageSize_);
        if (Y_UNLIKELY(TAlignedPagePool::IsDefaultAllocatorUsed())) {
            UnmapPage(addr);
            return;
        }
        ++Count_;
        Pages_.Enqueue(addr);
    }

    bool DiscardPage() {
        void* page = GetCachedPage();
        if (!page) {
            return false;
        }
        auto frozen = TFrozenPage<T>::Freeze(Provider_, page, PageSize_);
        if (!frozen) {
            ++Count_;
            Pages_.Enqueue(page);
            ythrow std::move(frozen).error();
        }
        DecommittedPages_.Enqueue(std::move(*frozen));
        return true;
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
    void* GetCachedPage() {
        void* page = nullptr;
        if (Pages_.Dequeue(&page)) {
            --Count_;
            return page;
        }

        return nullptr;
    }

    void* GetDecommittedPage() {
        TFrozenPage<T> page;
        if (!DecommittedPages_.Dequeue(&page)) {
            return nullptr;
        }
        auto result = std::move(page).Unlock();
        if (result) {
            return result->Value();
        }
        auto [frozenPage, error] = std::move(result).error().Value();
        DecommittedPages_.Enqueue(std::move(frozenPage));
        ythrow std::move(error);
    }

    void UnmapPage(void* addr) noexcept {
        auto result = Provider_.Munmap(addr, PageSize_, /*frozen=*/false);
        Y_DEBUG_ABORT_UNLESS(result, "%s", result.error().what());
    }

    T& Provider_;
    const size_t PageSize_;
    std::atomic<ui64> Count_ = 0;
    TLockFreeStack<void*> Pages_;
    TLockFreeStack<TFrozenPage<T>> DecommittedPages_;
};

} // namespace NKikimr
