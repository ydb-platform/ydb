#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("DuplicateFiltering/SourcesMerging/RowsBulkAccepted"))
    , IntersectingPortionsPerRequest(TBase::GetHistogram("DuplicateFiltering/IntersectingPortions", NMonitoring::ExponentialHistogram(18, 2, 8)))
    , FilterCacheHits(TBase::GetDeriviative("DuplicateFiltering/FilterCache/Hits"))
    , FilterCacheMisses(TBase::GetDeriviative("DuplicateFiltering/FilterCache/Misses"))
{
}
}   // namespace NKikimr::NColumnShard
