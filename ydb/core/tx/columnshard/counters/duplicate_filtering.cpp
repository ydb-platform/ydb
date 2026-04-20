#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsBulkAccepted"))
    , MergeQueue(TBase::GetValue("DuplicateFiltering/SourcesMerging/Queue"))
    , LeftBorders(TBase::GetValue("DuplicateFiltering/Borders/Left"))
    , WaitingBorders(TBase::GetValue("DuplicateFiltering/Borders/Waiting"))
    , ReadyBorders(TBase::GetValue("DuplicateFiltering/Borders/Ready"))
    , BordersPerRequest(TBase::GetHistogram("DuplicateFiltering/Borders/CountPerRequest", NMonitoring::ExponentialHistogram(18, 2, 1)))
    , RequestCacheHits(TBase::GetDeriviative("DuplicateFiltering/Request/CacheHits"))
    , RequestCacheMisses(TBase::GetDeriviative("DuplicateFiltering/Request/CacheMisses"))
    , RequestCacheWaiting(TBase::GetDeriviative("DuplicateFiltering/Request/CacheWaiting"))
    , RequestInflight(TBase::GetValue("DuplicateFiltering/Request/Inflight"))
    , RequestLatency(TBase::GetHistogram("DuplicateFiltering/Request/LatencyMs", NMonitoring::ExponentialHistogram(18, 2, 1)))
    , ExclusiveFilters(TBase::GetValue("DuplicateFiltering/Filters/Exclusive"))
    , ReadyFiltersCount(TBase::GetValue("DuplicateFiltering/Filters/ReadyFiltersCount"))
    , ReadyFiltersSize(TBase::GetValue("DuplicateFiltering/Filters/ReadyFiltersSize"))
{
}
}   // namespace NKikimr::NColumnShard
