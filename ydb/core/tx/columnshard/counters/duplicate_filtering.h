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
    NMonitoring::TDynamicCounters::TCounterPtr MergeQueue;
    
    NMonitoring::TDynamicCounters::TCounterPtr LeftBorders;
    NMonitoring::TDynamicCounters::TCounterPtr WaitingBorders;
    NMonitoring::TDynamicCounters::TCounterPtr ReadyBorders;
    NMonitoring::THistogramPtr BordersPerRequest;
    
    NMonitoring::TDynamicCounters::TCounterPtr RequestCacheHits;
    NMonitoring::TDynamicCounters::TCounterPtr RequestCacheMisses;
    NMonitoring::TDynamicCounters::TCounterPtr RequestCacheWaiting;
    NMonitoring::TDynamicCounters::TCounterPtr RequestInflight;
    
     NMonitoring::THistogramPtr RequestLatency;
     
    NMonitoring::TDynamicCounters::TCounterPtr ExclusiveFilters;
    NMonitoring::TDynamicCounters::TCounterPtr ReadyFiltersCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReadyFiltersSize;

public:
    TDuplicateFilteringCounters();

    void OnRowsMerged(const ui64 accepted, const ui64 rejected, const ui64 bulkAccepted) const {
        MergeRowsAccepted->Add(accepted);
        MergeRowsRejected->Add(rejected);
        MergeRowsBulkAccepted->Add(bulkAccepted);
    }
    
    void OnLeftBorders(const i64 count = 1) const {
        LeftBorders->Add(count);
    }
    
    void OnWaitingBorders(const i64 count = 1) const {
        WaitingBorders->Add(count);
    }
    
    void OnReadyBorders(const i64 count = 1) const {
        ReadyBorders->Add(count);
    }
    
    void OnBordersPerRequest(const i64 count = 1) const {
        BordersPerRequest->Collect(count);
    }
    
    void OnRequestCacheHit(const ui64 count = 1) const {
        RequestCacheHits->Add(count);
    }

    void OnRequestCacheMiss(const ui64 count = 1) const {
        RequestCacheMisses->Add(count);
    }
    
    void OnRequestCacheWaiting(const ui64 count = 1) const {
        RequestCacheWaiting->Add(count);
    }
    
    void OnRequestStart() const {
        RequestInflight->Add(1);
    }
    
    void OnRequestFinish(ui64 latencyMs) const {
        RequestLatency->Collect(latencyMs);
         RequestInflight->Add(-1);
    }
    
    void OnReadyFilters(i64 count, i64 size) const {
        ReadyFiltersCount->Add(count);
        ReadyFiltersSize->Add(size);
    }
    
    void OnExclusiveFilters(i64 count = 1) const {
        ExclusiveFilters->Add(count);
    }
    
    void OnMergeQueue(i64 count = 1) const {
        MergeQueue->Add(count);
    }
};

class TSimpleDuplicateFilteringCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsAccepted;
    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsRejected;
    NMonitoring::TDynamicCounters::TCounterPtr MergeRowsBulkAccepted;

    NMonitoring::THistogramPtr IntersectingPortionsPerRequest;

    NMonitoring::TDynamicCounters::TCounterPtr FilterCacheHits;
    NMonitoring::TDynamicCounters::TCounterPtr FilterCacheMisses;

public:
    TSimpleDuplicateFilteringCounters();

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
};
}   // namespace NKikimr::NColumnShard
