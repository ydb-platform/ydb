#pragma once

#include <ydb/library/signals/histogram.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {
class TDuplicateFilteringCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsAccepted;
    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsRejected;
    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsBulkAccepted;

    NMonitoring::THistogramPtr IntersectingPortionsPerRequest;

    NMonitoring::TDynamicCounters::TCounterPtr FilterCacheHits;
    NMonitoring::TDynamicCounters::TCounterPtr FilterCacheMisses;

    NMonitoring::TDynamicCounters::TCounterPtr FilterPortionsCacheHits;
    NMonitoring::TDynamicCounters::TCounterPtr FilterPortionsCacheMisses;

    NMonitoring::TDynamicCounters::TCounterPtr FetchedSources;

public:
    TDuplicateFilteringCounters();

    void OnRowsMerged(const ui64 accepted, const ui64 rejected, const ui64 bulkAccepted) const {
        MergeRowsAccepted->Add(accepted);
        MergeRowsRejected->Add(rejected);
        MergeRowsBulkAccepted->Add(bulkAccepted);
    }

    void OnFilterRequest(const ui64 intersectingPortions) const {
        IntersectingPortionsPerRequest->Collect(intersectingPortions);
    }

    void OnFilterCacheHit(const ui64 count = 1) const {
        FilterCacheHits->Add(count);
    }
    void OnFilterCacheMiss(const ui64 count = 1) const {
        FilterCacheMisses->Add(count);
    }

    void OnFilterPortionsCacheHit(const ui64 count = 1) const {
        FilterPortionsCacheHits->Add(count);
    }
    void OnFilterPortionsCacheMiss(const ui64 count = 1) const {
        FilterPortionsCacheMisses->Add(count);
    }

    void OnFetchedSources(const ui64 count = 1) const {
        FetchedSources->Add(count);
    }
};
}   // namespace NKikimr::NColumnShard
