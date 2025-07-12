#include "duplicate_filtering.h"

namespace NKikimr::NColumnShard {
TDuplicateFilteringCounters::TDuplicateFilteringCounters()
    : TBase("DuplicateFiltering")
    , MergeRowsAccepted(TBase::GetDeriviative("SourcesMerging/RowsAccepted"))
    , MergeRowsRejected(TBase::GetDeriviative("SourcesMerging/RowsRejected"))
    , MergeRowsBulkAccepted(TBase::GetDeriviative("SourcesMerging/RowsBulkAccepted")) {
}
}   // namespace NKikimr::NColumnShard
