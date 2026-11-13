#pragma once

#include "aligned_page_pool.h"
#include "global_page_pool.h"
#include "page_pool_constants.h"

#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <yql/essentials/public/udf/sanitizer_utils/sanitizer_utils.h>

#include <cstddef>
#include <utility>

namespace NKikimr {

template <typename T, bool SysAlign>
class TGlobalPools {
    using TPagePool = TGlobalPagePool<T, SysAlign>;

public:
    static TGlobalPools<T, SysAlign>& Instance() {
        return *Singleton<TGlobalPools<T, SysAlign>>();
    }

    TPagePool& Get(ui32 index) {
        return *Pools_[index];
    }

    const TPagePool& Get(ui32 index) const {
        return *Pools_[index];
    }

    TGlobalPools()
        : Provider_(T::GetInstance())
    {
        Reset();
    }

    void* DoMmap(size_t size) {
        Y_DEBUG_ABORT_UNLESS(!TAlignedPagePoolImpl<T>::IsDefaultAllocatorUsed(), "No memory maps allowed while using default allocator");

        auto result = Provider_.Mmap(size);
        if (Y_UNLIKELY(!result)) {
            ythrow std::move(result).error();
        }
        NYql::NUdf::SanitizerMakeRegionInaccessible(*result, size);
        return *result;
    }

    void DoCleanupFreeList(ui64 targetSize) {
        for (ui32 level = 0; level <= MidLevels; ++level) {
            auto& pool = Get(level);
            while (pool.GetSize() >= targetSize) {
                if (!pool.DiscardPage()) {
                    break;
                }
            }
        }
    }

    void PushPage(size_t level, void* addr) {
        Get(level).PushPage(addr);
    }

    void DoMunmap(void* addr, size_t size) {
        auto result = Provider_.Munmap(addr, size, /*frozen=*/false);
        if (Y_UNLIKELY(!result)) {
            ythrow std::move(result).error();
        }
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
            Pools_.emplace_back(MakeHolder<TPagePool>(Provider_, TAlignedPagePool::POOL_PAGE_SIZE << i));
        }
    }

private:
    T& Provider_;
    TVector<THolder<TPagePool>> Pools_;
};

} // namespace NKikimr
