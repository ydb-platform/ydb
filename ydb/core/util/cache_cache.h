#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

enum class ECacheCacheGeneration {
    None,
    Fresh,
    Staging,
    Warm,
};

struct TCacheCacheConfig : public TAtomicRefCount<TCacheCacheConfig> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

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

template <typename TItem, typename TItemTraits>
class TCacheCache : public ICacheCache<TItem> {
public:
    TCacheCache(const TCacheCacheConfig &config)
        : Config(config)
        , FreshWeight(0)
        , StagingWeight(0)
        , WarmWeight(0)
    {}

    TIntrusiveList<TItem> EvictNext() override {
        TIntrusiveList<TItem> evictedList;
        
        if (!StagingList.Empty()) {
            evictedList.PushBack(EvictNext(StagingList, StagingWeight));
            if (Config.ReportedStaging)
                *Config.ReportedStaging = StagingWeight;
        } else if (!FreshList.Empty()) {
            evictedList.PushBack(EvictNext(FreshList, FreshWeight));
            if (Config.ReportedFresh)
                *Config.ReportedFresh = FreshWeight;
        } else if (!WarmList.Empty()) {
            evictedList.PushBack(EvictNext(WarmList, WarmWeight));
            if (Config.ReportedWarm)
                *Config.ReportedWarm = WarmWeight;
        }

        return evictedList;
    }

    // returns evicted elements as list
    TIntrusiveList<TItem> Touch(TItem *item) override {
        TIntrusiveList<TItem> evictedList;
        TIntrusiveListItem<TItem> *xitem = item;

        const ECacheCacheGeneration cacheGen = TItemTraits::GetGeneration(item);
        switch (cacheGen) {
        case ECacheCacheGeneration::None: // place in fresh
            AddToFresh(item, evictedList);
	    [[fallthrough]];
        case ECacheCacheGeneration::Fresh: // just update inside fresh
            xitem->Unlink();
            FreshList.PushFront(xitem);
            break;
        case ECacheCacheGeneration::Staging: // move to warm
            MoveToWarm(item, evictedList);
            break;
        case ECacheCacheGeneration::Warm: // just update inside warm
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
        const ECacheCacheGeneration cacheGen = TItemTraits::GetGeneration(item);
        switch (cacheGen) {
        case ECacheCacheGeneration::None:
            break;
        case ECacheCacheGeneration::Fresh:
            Unlink(item, FreshWeight);
            if (Config.ReportedFresh)
                *Config.ReportedFresh = FreshWeight;
            break;
        case ECacheCacheGeneration::Staging:
            Unlink(item, StagingWeight);
            if (Config.ReportedStaging)
                *Config.ReportedStaging = StagingWeight;
            break;
        case ECacheCacheGeneration::Warm:
            Unlink(item, WarmWeight);
            if (Config.ReportedWarm)
                *Config.ReportedWarm = WarmWeight;
            break;
        default:
            Y_DEBUG_ABORT("unknown cache generation");
        }
        TItemTraits::SetGeneration(item, ECacheCacheGeneration::None);
    }

    void UpdateLimit(ui64 limit) override {
        Config.SetLimit(limit);
    }

    ui64 GetSize() const override {
        return FreshWeight + StagingWeight + WarmWeight;
    }

private:
    void Unlink(TItem *item, ui64 &weight) {
        item->Unlink();

        const ui64 elementWeight = TItemTraits::GetWeight(item);
        Y_DEBUG_ABORT_UNLESS(elementWeight <= weight);
        weight -= elementWeight;
    }

    void AddToFresh(TItem *item, TIntrusiveList<TItem>& evictedList) {
        LimitFresh(evictedList);
        item->Unlink();
        FreshWeight += TItemTraits::GetWeight(item);
        FreshList.PushFront(item);
        TItemTraits::SetGeneration(item, ECacheCacheGeneration::Fresh);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedFresh)
            *Config.ReportedFresh = FreshWeight;
    }

    void MoveToWarm(TItem *item, TIntrusiveList<TItem>& evictedList) {
        // Note: unlink first, so item is not evicted by LimitWarm call below
        Unlink(item, StagingWeight);
        LimitWarm(evictedList);
        WarmWeight += TItemTraits::GetWeight(item);
        WarmList.PushFront(item);
        TItemTraits::SetGeneration(item, ECacheCacheGeneration::Warm);

        if (Config.ReportedStaging)
            *Config.ReportedStaging = StagingWeight;
        if (Config.ReportedWarm)
            *Config.ReportedWarm = WarmWeight;
    }

    void AddToStaging(TItem *item, TIntrusiveList<TItem>& evictedList) {
        LimitStaging(evictedList);
        StagingWeight += TItemTraits::GetWeight(item);
        StagingList.PushFront(item);
        TItemTraits::SetGeneration(item, ECacheCacheGeneration::Staging);
    }

    void LimitFresh(TIntrusiveList<TItem>& evictedList) {
        while (FreshWeight > Config.FreshLimit) {
            Y_DEBUG_ABORT_UNLESS(!FreshList.Empty());
            TItem *x = FreshList.PopBack();
            Y_ABORT_UNLESS(TItemTraits::GetGeneration(x) == ECacheCacheGeneration::Fresh, "malformed entry in fresh cache. %" PRIu32, (ui32)TItemTraits::GetGeneration(x));
            Unlink(x, FreshWeight);
            AddToStaging(x, evictedList);
        }
    }

    void LimitWarm(TIntrusiveList<TItem>& evictedList) {
        while (WarmWeight > Config.WarmLimit) {
            Y_DEBUG_ABORT_UNLESS(!WarmList.Empty());
            TItem *x = WarmList.PopBack();
            Y_ABORT_UNLESS(TItemTraits::GetGeneration(x) == ECacheCacheGeneration::Warm, "malformed entry in warm cache. %" PRIu32, (ui32)TItemTraits::GetGeneration(x));
            Unlink(x, WarmWeight);
            AddToStaging(x, evictedList);
        }
    }

    void LimitStaging(TIntrusiveList<TItem>& evictedList) {
        while (StagingWeight > Config.StagingLimit) {
            Y_DEBUG_ABORT_UNLESS(!StagingList.Empty());
            TItem *evicted = StagingList.PopBack();
            Y_ABORT_UNLESS(TItemTraits::GetGeneration(evicted) == ECacheCacheGeneration::Staging, "malformed entry in staging cache %" PRIu32, (ui32)TItemTraits::GetGeneration(evicted));
            Unlink(evicted, StagingWeight);
            TItemTraits::SetGeneration(evicted, ECacheCacheGeneration::None);
            evictedList.PushBack(evicted);
        }
    }

    TItem* EvictNext(TIntrusiveList<TItem>& list, ui64& weight) {
        Y_DEBUG_ABORT_UNLESS(!list.Empty());

        TItem *evicted = list.PopBack();
        Unlink(evicted, weight);
        TItemTraits::SetGeneration(evicted, ECacheCacheGeneration::None);

        return evicted;
    }

private:
    TCacheCacheConfig Config;

    TIntrusiveList<TItem> FreshList;
    TIntrusiveList<TItem> StagingList;
    TIntrusiveList<TItem> WarmList;

    ui64 FreshWeight;
    ui64 StagingWeight;
    ui64 WarmWeight;
};

}
