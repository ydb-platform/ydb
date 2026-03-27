#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsBulkAccepted"))
    , LeftBorders(TBase::GetValue("DuplicateFiltering/Borders/Left"))
    , WaitingBorders(TBase::GetValue("DuplicateFiltering/Borders/Waiting"))
    , ReadyBorders(TBase::GetValue("DuplicateFiltering/Borders/Ready"))
    , IntervalsPerRequest(TBase::GetHistogram("DuplicateFiltering/Intervals/CountPerRequest", NMonitoring::ExponentialHistogram(18, 2, 1)))
    , RequestCacheHits(TBase::GetDeriviative("DuplicateFiltering/Request/CacheHits"))
    , RequestCacheMisses(TBase::GetDeriviative("DuplicateFiltering/Request/CacheMisses"))
    , RequestCacheWaiting(TBase::GetDeriviative("DuplicateFiltering/Request/CacheWaiting"))
    , RequestInflight(TBase::GetValue("DuplicateFiltering/Request/Inflight"))
    , RequestLatency(TBase::GetHistogram("DuplicateFiltering/Request/LatencyMs", NMonitoring::ExponentialHistogram(18, 2, 1)))
{
}
}   // namespace NKikimr::NColumnShard
