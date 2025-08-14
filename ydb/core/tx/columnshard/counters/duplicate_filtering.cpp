#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("SourcesMerging/RowsBulkAccepted"))
    , IntersectingPortionsPerRequest(TBase::GetHistogram("SourcesMerging/IntersectingPortions", NMonitoring::ExponentialHistogram(18, 2, 8)))
    , FilterCacheHits(TBase::GetDeriviative("SourcesMerging/FilterCache/Hits"))
    , FilterCacheMisses(TBase::GetDeriviative("SourcesMerging/FilterCache/Misses"))
{
}
}   // namespace NKikimr::NColumnShard
