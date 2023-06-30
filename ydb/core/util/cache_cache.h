#pragma once
#include "defs.h"
#include <ydb/core/util/queue_oneone_inplace.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr {

struct TCacheCacheConfig : public TAtomicRefCount<TCacheCacheConfig> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    enum ECacheGeneration {
        CacheGenNone,
        CacheGenEvicted,
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

    template<typename TItem>
    struct TDefaultWeight {
        static ui64 Get(TItem *) {
            return 1;
        }
    };

    template<typename TItem>
    struct TDefaultGeneration {
        static ECacheGeneration Get(TItem *x) {
            return static_cast<ECacheGeneration>(x->CacheGeneration);
        }
        static void Set(TItem *x, ECacheGeneration gen) {
            x->CacheGeneration = gen;
        }
    };
};

template <typename TItem
        , typename TWeight = TCacheCacheConfig::TDefaultWeight<TItem>
        , typename TGeneration = TCacheCacheConfig::TDefaultGeneration<TItem>
    >
class TCacheCache {
public:
    TCacheCache(const TCacheCacheConfig &config)
        : Config(config)
        , FreshWeight(0)
        , StagingWeight(0)
        , WarmWeight(0)
    {}

    // returns evicted elements as list
    TItem* EnsureLimits() {
        TItem *ret = nullptr;
        ret = LimitFresh(ret);
        ret = LimitWarm(ret);
        ret = LimitStaging(ret);

        if (ret) {
            if (Config.ReportedFresh)
                *Config.ReportedFresh = FreshWeight;
            if (Config.ReportedWarm)
                *Config.ReportedWarm = WarmWeight;
            if (Config.ReportedStaging)
                *Config.ReportedStaging = StagingWeight;
        }

        return ret;
    }

    // returns evicted elements as list
    TItem* Touch(TItem *item) {
        TIntrusiveListItem<TItem> *xitem = item;

        const TCacheCacheConfig::ECacheGeneration cacheGen = GenerationOp.Get(item);
        switch (cacheGen) {
        case TCacheCacheConfig::CacheGenNone: // place in fresh
        case TCacheCacheConfig::CacheGenEvicted: // corner case: was evicted from staging and touched in same update
            return AddToFresh(item);
        case TCacheCacheConfig::CacheGenFresh: // just update inside fresh
            xitem->Unlink();
            FreshList.PushFront(xitem);
            return nullptr;
        case TCacheCacheConfig::CacheGenStaging: // move to warm
            return MoveToWarm(item);
        case TCacheCacheConfig::CacheGenWarm: // just update inside warm
            xitem->Unlink();
            WarmList.PushFront(xitem);
            return nullptr;
        default:
            Y_VERIFY_DEBUG(false, "unknown/broken cache generation");
            return nullptr;
        }
    }

    // evict and erase differs on Evicted handling
    void Evict(TItem *item) {
        const TCacheCacheConfig::ECacheGeneration cacheGen = GenerationOp.Get(item);
        switch (cacheGen) {
        case TCacheCacheConfig::CacheGenNone:
        case TCacheCacheConfig::CacheGenEvicted:
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
            Y_VERIFY_DEBUG(false, "unknown cache generaton");
        }
    }

    void Erase(TItem *item) {
        const TCacheCacheConfig::ECacheGeneration cacheGen = GenerationOp.Get(item);
        switch (cacheGen) {
        case TCacheCacheConfig::CacheGenNone:
            break;
        case TCacheCacheConfig::CacheGenEvicted:
            item->Unlink();
            GenerationOp.Set(item, TCacheCacheConfig::CacheGenNone);
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
            Y_VERIFY_DEBUG(false, "unknown cache generaton");
        }
    }

    void UpdateCacheSize(ui64 cacheSize) {
        if (cacheSize == 0)
            cacheSize = Max<ui64>();

        Config.SetLimit(cacheSize);
    }
private:
    void Unlink(TItem *item, ui64 &weight) {
        item->Unlink();

        const ui64 elementWeight = WeightOp.Get(item);
        Y_VERIFY_DEBUG(elementWeight <= weight);
        weight -= elementWeight;
    }

    TItem* AddToFresh(TItem *item) {
        TItem *ret = LimitFresh(nullptr);
        item->Unlink();
        FreshWeight += WeightOp.Get(item);
        FreshList.PushFront(item);
        GenerationOp.Set(item, TCacheCacheConfig::CacheGenFresh);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedFresh)
            *Config.ReportedFresh = FreshWeight;

        return ret;
    }

    TItem* MoveToWarm(TItem *item) {
        TItem *ret = LimitWarm(nullptr);
        Unlink(item, StagingWeight);
        WarmWeight += WeightOp.Get(item);
        WarmList.PushFront(item);
        GenerationOp.Set(item, TCacheCacheConfig::CacheGenWarm);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedWarm)
            *Config.ReportedWarm = WarmWeight;

        return ret;
    }

    TItem* AddToStaging(TItem *item, TItem *ret) {
        ret = LimitStaging(ret);
        StagingWeight += WeightOp.Get(item);
        StagingList.PushFront(item);
        GenerationOp.Set(item, TCacheCacheConfig::CacheGenStaging);
        return ret;
    }

    TItem* LimitFresh(TItem *ret) {
        while (FreshWeight > Config.FreshLimit) {
            Y_VERIFY_DEBUG(!FreshList.Empty());
            TItem *x = FreshList.PopBack();
            Y_VERIFY(GenerationOp.Get(x) == TCacheCacheConfig::CacheGenFresh, "malformed entry in fresh cache. %" PRIu32, (ui32)GenerationOp.Get(x));
            Unlink(x, FreshWeight);
            ret = AddToStaging(x, ret);
        }
        return ret;
    }

    TItem* LimitWarm(TItem *ret) {
        while (WarmWeight > Config.WarmLimit) {
            Y_VERIFY_DEBUG(!WarmList.Empty());
            TItem *x = WarmList.PopBack();
            Y_VERIFY(GenerationOp.Get(x) == TCacheCacheConfig::CacheGenWarm, "malformed entry in warm cache. %" PRIu32, (ui32)GenerationOp.Get(x));
            Unlink(x, WarmWeight);
            ret = AddToStaging(x, ret);
        }
        return ret;
    }

    TItem* LimitStaging(TItem *ret) {
        while (StagingWeight > Config.StagingLimit) {
            Y_VERIFY_DEBUG(!StagingList.Empty());
            TItem *evicted = StagingList.PopBack();
            Y_VERIFY(GenerationOp.Get(evicted) == TCacheCacheConfig::CacheGenStaging, "malformed entry in staging cache %" PRIu32, (ui32)GenerationOp.Get(evicted));
            Unlink(evicted, StagingWeight);
            GenerationOp.Set(evicted, TCacheCacheConfig::CacheGenEvicted);
            if (ret == nullptr)
                ret = evicted;
            else
                evicted->LinkBefore(ret);
        }
        return ret;
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
    TGeneration GenerationOp;
};

}
