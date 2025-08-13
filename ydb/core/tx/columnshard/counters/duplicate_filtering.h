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
};
}   // namespace NKikimr::NColumnShard
