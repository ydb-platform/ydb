#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("SourcesMerging/RowsBulkAccepted"))
    , SourceCacheHits(TBase::GetDeriviative("DuplicateFiltration/SourceCache/Hits"))
    , SourceCacheMisses(TBase::GetDeriviative("DuplicateFiltration/SourceCache/Misses")) {
}
}   // namespace NKikimr::NColumnShard
