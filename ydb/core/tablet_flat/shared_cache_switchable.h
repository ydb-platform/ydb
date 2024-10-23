#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

template <typename TPage, typename TPageTraits>
class TSwitchableCache : public ICacheCache<TPage> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    static const ui32 MaxCachesCount = 3;
    static const ui32 RotatePagesPerCallCount = 10;
    static_assert(MaxCachesCount < (1 << 4));

    class TCacheHolder {
    public:
        TCacheHolder(ui32 id, THolder<ICacheCache<TPage>>&& cache, TCounterPtr& sizeCounter)
            : Id(id)
            , Cache(std::move(cache))
            , SizeCounter(sizeCounter)
        {
            Y_ABORT_UNLESS(GetSize() == 0);
        }

        TIntrusiveList<TPage> EvictNext() {
            return ProcessEvictedList(Cache->EvictNext());
        }

        TIntrusiveList<TPage> Touch(TPage* page) {
            ui32 cacheId = TPageTraits::GetCacheId(page);
            if (cacheId == 0) {
                TPageTraits::SetCacheId(page, Id);
                SizeCounter->Add(TPageTraits::GetSize(page));
            } else {
                Y_ABORT_UNLESS(cacheId == Id);
            }

            return ProcessEvictedList(Cache->Touch(page));
        }

        void Erase(TPage* page) {            
            ui32 cacheId = TPageTraits::GetCacheId(page);
            if (cacheId != 0) {
                Y_ABORT_UNLESS(cacheId == Id);
                SizeCounter->Sub(TPageTraits::GetSize(page));
                TPageTraits::SetCacheId(page, 0);
            }

            Cache->Erase(page);
        }

        void UpdateLimit(ui64 limit) {
            Cache->UpdateLimit(limit);
        }

        ui64 GetSize() const {
            return Cache->GetSize();
        }

        TString Dump() const {
            return Cache->Dump();
        }

    private:
        TIntrusiveList<TPage> ProcessEvictedList(TIntrusiveList<TPage>&& evictedList) {
            ui64 evictedSize = 0;

            for (auto& page_ : evictedList) {
                TPage* page = &page_;
                Y_ABORT_UNLESS(TPageTraits::GetCacheId(page) == Id);
                TPageTraits::SetCacheId(page, 0);
                evictedSize += TPageTraits::GetSize(page);
            }

            SizeCounter->Sub(evictedSize);

            return evictedList;
        }

    public:
        const ui32 Id; // in [1 .. MaxCachesCount] range

    private:
        const THolder<ICacheCache<TPage>> Cache;
        const TCounterPtr SizeCounter;
    };

public:
    TSwitchableCache(ui64 limit, THolder<ICacheCache<TPage>>&& cache, TCounterPtr sizeCounter) {
        Caches.emplace_back(1, std::move(cache), sizeCounter);
        UpdateLimit(limit);
    }

    TIntrusiveList<TPage> Switch(THolder<ICacheCache<TPage>>&& cache, TCounterPtr sizeCounter) Y_WARN_UNUSED_RESULT {
        ui32 cacheId = Caches.back().Id + 1;
        if (cacheId > MaxCachesCount) {
            cacheId -= MaxCachesCount;
        }

        Caches.emplace_back(cacheId, std::move(cache), sizeCounter)
            .UpdateLimit(Limit);

        TIntrusiveList<TPage> evictedList;

        while (Caches.size() > 1 && Caches.front().Id == cacheId) { // MaxCachesCount is exceeded
            RotatePages(evictedList);
        }

        return evictedList;
    }

    TIntrusiveList<TPage> EvictNext() override {
        while (Y_UNLIKELY(Caches.size() > 1)) {
            auto result = Caches.front().EvictNext();
            if (!result) {
                Y_ABORT_UNLESS(Caches.front().GetSize() == 0);
                Caches.pop_front();
            } else {
                return result;
            }
        }

        return Caches.back().EvictNext();
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        if (Y_LIKELY(Caches.size() == 1)) {
            return Caches.back().Touch(page);
        }

        ui32 cacheId = TPageTraits::GetCacheId(page);
        if (cacheId > 0 && cacheId != Caches.back().Id) {
            // rotate the current page first:
            GetCache(cacheId).Erase(page);
        }
        
        TIntrusiveList<TPage> evictedList = Caches.back().Touch(page);

        RotatePages(evictedList);

        while (GetSize() > Limit && Caches.size() > 1) {
            evictedList.Append(EvictNext());
        }

        return evictedList;
    }

    void Erase(TPage* page) override {
        if (Y_LIKELY(Caches.size() == 1)) {
            Caches.back().Erase(page);
            return;
        }

        GetCache(TPageTraits::GetCacheId(page))
            .Erase(page);
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        for (auto& cache : Caches) {
            cache.UpdateLimit(limit);
        }
    }

    ui64 GetSize() const override {
        ui64 result = 0;
        for (const auto& cache : Caches) {
            result += cache.GetSize();
        }
        return result;
    }

    TString Dump() const override {
        TStringBuilder result;
        size_t count = 0;

        for (const auto& cache : Caches) {
            if (count) result << "; ";
            result << cache.Dump();
            count++;
        }
    
        return result;
    }

private:
    TCacheHolder& GetCache(ui32 cacheId) {
        if (cacheId == 0) {
            // use the most-recent cache by default
            return Caches.back();
        } else {
            // Note: this loop might be replaced with formula
            // but it seems useless and error-prone
            for (auto& cache : Caches) {
                if (cache.Id == cacheId) {
                    return cache;
                }
            }
            Y_ABORT("Failed to locate page cache");
        }
    }

    void RotatePages(TIntrusiveList<TPage>& evictedList) {
        ui32 rotatedPagesCount = 0;
        while (Caches.size() > 1 && rotatedPagesCount < RotatePagesPerCallCount) {
            auto rotatedList = Caches.front().EvictNext();
            if (!rotatedList) {
                Y_ABORT_UNLESS(Caches.front().GetSize() == 0);
                Caches.pop_front();
                continue;
            }

            while (!rotatedList.Empty()) {
                TPage* page = rotatedList.PopFront();
                evictedList.Append(Caches.back().Touch(page));
                rotatedPagesCount++;
            }
        }
    }

private:
    ui64 Limit;
    TDeque<TCacheHolder> Caches;
};

}
