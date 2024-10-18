#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

template <typename TPage>
class TCompositeCache : public ICacheCache<TPage> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    struct TCacheHolder {
        THolder<ICacheCache<TPage>> Cache;
        TCounterPtr SizeCounter; // TODO: update
    };

public:
    TCompositeCache(THolder<ICacheCache<TPage>>&& cache, TCounterPtr sizeCounter) {
        Caches.emplace_back(std::move(cache), sizeCounter);
    }

    void Switch(THolder<ICacheCache<TPage>>&& cache, TCounterPtr sizeCounter) {
        while (!Caches.empty() && Caches.front().Cache->GetSize() == 0) {
            Caches.pop_front();
        }

        cache->UpdateLimit(Limit);
        Caches.emplace_back(std::move(cache), sizeCounter);
    }

    TIntrusiveList<TPage> EvictNext() override {
        while (Y_UNLIKELY(Caches.size() > 1)) {
            auto result = Caches.front().Cache->EvictNext();
            if (!result) {
                Y_ABORT_UNLESS(Caches.front().Cache->GetSize() == 0);
                Caches.pop_front();
            }
        }

        return Caches.front().Cache->EvictNext();;
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        if (Y_LIKELY(Caches.size() == 1)) {
            return Caches.front().Cache->Touch(page);
        }

        Y_ABORT("TODO");
    }

    void Erase(TPage* page) override {
        if (Y_LIKELY(Caches.size() == 1)) {
            return Caches.front().Cache->Erase(page);
        }

        Y_ABORT("TODO");
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        for (auto& cache : Caches) {
            cache.Cache->UpdateLimit(limit);
        }
    }

    ui64 GetSize() const override {
        ui64 result = 0;
        for (const auto& cache : Caches) {
            result += cache.Cache->GetSize();
        }
        return result;
    }

private:
    ui64 Limit = 0;
    TDeque<TCacheHolder> Caches;
};

}
