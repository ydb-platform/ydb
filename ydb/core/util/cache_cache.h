#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

struct TCacheCacheConfig : public TAtomicRefCount<TCacheCacheConfig> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    enum ECacheGeneration {
        CacheGenNone,
        CacheGenFresh,
        CacheGenStaging,
        CacheGenWarm,
    };

    ui64 Limit;

    ui64 FreshLimit;
    ui64 StagingLimit;
    ui64 WarmLimit;

    TCounterPtr ReportedFresh;
    TCounterPtr ReportedStaging;
    TCounterPtr ReportedWarm;

    TCacheCacheConfig(ui64 limit, const TCounterPtr &reportedFresh, const TCounterPtr &reportedStaging, const TCounterPtr &reportedWarm)
        : Limit(0)
        , FreshLimit(0)
        , StagingLimit(0)
        , WarmLimit(0)
        , ReportedFresh(reportedFresh)
        , ReportedStaging(reportedStaging)
        , ReportedWarm(reportedWarm)
    {
        SetLimit(limit);
    }

    void SetLimit(ui64 limit) {
        Limit = limit;

        FreshLimit = Limit / 3;
        StagingLimit = FreshLimit;
        WarmLimit = FreshLimit;
    }
};

template <typename TItem
        , typename TWeight
        , typename TCacheFlags
    >
class TCacheCache : public ICacheCache<TItem> {
public:
    TCacheCache(const TCacheCacheConfig &config)
        : Config(config)
        , FreshWeight(0)
        , StagingWeight(0)
        , WarmWeight(0)
    {}

    TItem* EvictNext() override {
        TItem* ret = nullptr;

        if (!StagingList.Empty()) {
            ret = EvictNext(StagingList, StagingWeight);
            if (Config.ReportedStaging)
                *Config.ReportedStaging = StagingWeight;
        } else if (!FreshList.Empty()) {
            ret = EvictNext(FreshList, FreshWeight);
            if (Config.ReportedFresh)
                *Config.ReportedFresh = FreshWeight;
        } else if (!WarmList.Empty()) {
            ret = EvictNext(WarmList, WarmWeight);
            if (Config.ReportedWarm)
                *Config.ReportedWarm = WarmWeight;
        }

        return ret;
    }

    // returns evicted elements as list
    TIntrusiveList<TItem> Touch(TItem *item) override {
        TIntrusiveList<TItem> evictedList;
        TIntrusiveListItem<TItem> *xitem = item;

        const TCacheCacheConfig::ECacheGeneration cacheGen = GetGeneration(item);
        switch (cacheGen) {
        case TCacheCacheConfig::CacheGenNone: // place in fresh
            AddToFresh(item, evictedList);
	    [[fallthrough]];
        case TCacheCacheConfig::CacheGenFresh: // just update inside fresh
            xitem->Unlink();
            FreshList.PushFront(xitem);
            break;
        case TCacheCacheConfig::CacheGenStaging: // move to warm
            MoveToWarm(item, evictedList);
            break;
        case TCacheCacheConfig::CacheGenWarm: // just update inside warm
            xitem->Unlink();
            WarmList.PushFront(xitem);
            break;
        default:
            Y_DEBUG_ABORT("unknown/broken cache generation");
            break;
        }

        return evictedList;
    }

    void Erase(TItem *item) override {
        const TCacheCacheConfig::ECacheGeneration cacheGen = GetGeneration(item);
        switch (cacheGen) {
        case TCacheCacheConfig::CacheGenNone:
            break;
        case TCacheCacheConfig::CacheGenFresh:
            Unlink(item, FreshWeight);
            if (Config.ReportedFresh)
                *Config.ReportedFresh = FreshWeight;
            break;
        case TCacheCacheConfig::CacheGenStaging:
            Unlink(item, StagingWeight);
            if (Config.ReportedStaging)
                *Config.ReportedStaging = StagingWeight;
            break;
        case TCacheCacheConfig::CacheGenWarm:
            Unlink(item, WarmWeight);
            if (Config.ReportedWarm)
                *Config.ReportedWarm = WarmWeight;
            break;
        default:
            Y_DEBUG_ABORT("unknown cache generation");
        }
        SetGeneration(item, TCacheCacheConfig::CacheGenNone);
    }

    void UpdateCacheSize(ui64 cacheSize) override {
        Config.SetLimit(cacheSize);
    }

private:
    void Unlink(TItem *item, ui64 &weight) {
        item->Unlink();

        const ui64 elementWeight = WeightOp.Get(item);
        Y_DEBUG_ABORT_UNLESS(elementWeight <= weight);
        weight -= elementWeight;
    }

    void AddToFresh(TItem *item, TIntrusiveList<TItem>& evictedList) {
        LimitFresh(evictedList);
        item->Unlink();
        FreshWeight += WeightOp.Get(item);
        FreshList.PushFront(item);
        SetGeneration(item, TCacheCacheConfig::CacheGenFresh);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedFresh)
            *Config.ReportedFresh = FreshWeight;
    }

    void MoveToWarm(TItem *item, TIntrusiveList<TItem>& evictedList) {
        // Note: unlink first, so item is not evicted by LimitWarm call below
        Unlink(item, StagingWeight);
        LimitWarm(evictedList);
        WarmWeight += WeightOp.Get(item);
        WarmList.PushFront(item);
        SetGeneration(item, TCacheCacheConfig::CacheGenWarm);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedWarm)
            *Config.ReportedWarm = WarmWeight;
    }

    void AddToStaging(TItem *item, TIntrusiveList<TItem>& evictedList) {
        LimitStaging(evictedList);
        StagingWeight += WeightOp.Get(item);
        StagingList.PushFront(item);
        SetGeneration(item, TCacheCacheConfig::CacheGenStaging);
    }

    void LimitFresh(TIntrusiveList<TItem>& evictedList) {
        while (FreshWeight > Config.FreshLimit) {
            Y_DEBUG_ABORT_UNLESS(!FreshList.Empty());
            TItem *x = FreshList.PopBack();
            Y_ABORT_UNLESS(GetGeneration(x) == TCacheCacheConfig::CacheGenFresh, "malformed entry in fresh cache. %" PRIu32, (ui32)GetGeneration(x));
            Unlink(x, FreshWeight);
            AddToStaging(x, evictedList);
        }
    }

    void LimitWarm(TIntrusiveList<TItem>& evictedList) {
        while (WarmWeight > Config.WarmLimit) {
            Y_DEBUG_ABORT_UNLESS(!WarmList.Empty());
            TItem *x = WarmList.PopBack();
            Y_ABORT_UNLESS(GetGeneration(x) == TCacheCacheConfig::CacheGenWarm, "malformed entry in warm cache. %" PRIu32, (ui32)GetGeneration(x));
            Unlink(x, WarmWeight);
            AddToStaging(x, evictedList);
        }
    }

    void LimitStaging(TIntrusiveList<TItem>& evictedList) {
        while (StagingWeight > Config.StagingLimit) {
            Y_DEBUG_ABORT_UNLESS(!StagingList.Empty());
            TItem *evicted = StagingList.PopBack();
            Y_ABORT_UNLESS(GetGeneration(evicted) == TCacheCacheConfig::CacheGenStaging, "malformed entry in staging cache %" PRIu32, (ui32)GetGeneration(evicted));
            Unlink(evicted, StagingWeight);
            SetGeneration(evicted, TCacheCacheConfig::CacheGenNone);
            evictedList.PushBack(evicted);
        }
    }

    TItem* EvictNext(TIntrusiveList<TItem>& list, ui64& weight) {
        Y_DEBUG_ABORT_UNLESS(!list.Empty());

        TItem *evicted = list.PopBack();
        Unlink(evicted, weight);
        SetGeneration(evicted, TCacheCacheConfig::CacheGenNone);

        return evicted;
    }

    void SetGeneration(TItem *item, TCacheCacheConfig::ECacheGeneration gen) {
        GenerationOp.Set(item, static_cast<ui32>(gen));
    }

    TCacheCacheConfig::ECacheGeneration GetGeneration(TItem *item) {
        return static_cast<TCacheCacheConfig::ECacheGeneration>(GenerationOp.Get(item));
    }

private:
    TCacheCacheConfig Config;

    TIntrusiveList<TItem> FreshList;
    TIntrusiveList<TItem> StagingList;
    TIntrusiveList<TItem> WarmList;

    ui64 FreshWeight;
    ui64 StagingWeight;
    ui64 WarmWeight;

    TWeight WeightOp;
    TCacheFlags GenerationOp;
};

}
