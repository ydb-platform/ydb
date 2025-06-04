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

    NMonitoring::TDynamicCounters::TCounterPtr SourceCacheHits;
    NMonitoring::TDynamicCounters::TCounterPtr SourceCacheMisses;

public:
    TDuplicateFilteringCounters();

    void OnRowsMerged(const ui64 accepted, const ui64 rejected, const ui64 bulkAccepted) const {
        MergeRowsAccepted->Add(accepted);
        MergeRowsRejected->Add(rejected);
        MergeRowsBulkAccepted->Add(bulkAccepted);
    }

    void OnSourceCacheRequest(const ui64 hits, const ui64 misses) const {
        SourceCacheHits->Add(hits);
        SourceCacheMisses->Add(misses);
    }
};
}   // namespace NKikimr::NColumnShard
