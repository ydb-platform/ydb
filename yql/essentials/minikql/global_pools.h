#pragma once

#include "aligned_page_pool.h"
#include "global_page_pool.h"
#include "page_pool_constants.h"
#include "system_mmap.h"

#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/compiler.h>
#include <util/system/error.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <yql/essentials/public/udf/sanitizer_utils.h>

#include <atomic>
#include <cstddef>

namespace NKikimr {

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

} // namespace NKikimr
