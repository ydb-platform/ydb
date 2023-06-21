#include "scan.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module)
    : TBase(module)
    , ProcessingOverload(TBase::GetDeriviative("ProcessingOverload"))
    , ReadingOverload(TBase::GetDeriviative("ReadingOverload"))
    , PortionBytes(TBase::GetDeriviative("PortionBytes"))
    , FilterBytes(TBase::GetDeriviative("FilterBytes"))
    , PostFilterBytes(TBase::GetDeriviative("PostFilterBytes"))

    , AssembleFilterCount(TBase::GetDeriviative("AssembleFilterCount"))

    , FilterOnlyCount(TBase::GetDeriviative("FilterOnlyCount"))
    , FilterOnlyFetchedBytes(TBase::GetDeriviative("FilterOnlyFetchedBytes"))
    , FilterOnlyUsefulBytes(TBase::GetDeriviative("FilterOnlyUsefulBytes"))

    , EmptyFilterCount(TBase::GetDeriviative("EmptyFilterCount"))
    , EmptyFilterFetchedBytes(TBase::GetDeriviative("EmptyFilterFetchedBytes"))

    , OriginalRowsCount(TBase::GetDeriviative("OriginalRowsCount"))
    , FilteredRowsCount(TBase::GetDeriviative("FilteredRowsCount"))
    , SkippedBytes(TBase::GetDeriviative("SkippedBytes"))

    , TwoPhasesCount(TBase::GetDeriviative("TwoPhasesCount"))
    , TwoPhasesFilterFetchedBytes(TBase::GetDeriviative("TwoPhasesFilterFetchedBytes"))
    , TwoPhasesFilterUsefulBytes(TBase::GetDeriviative("TwoPhasesFilterUsefulBytes"))
    , TwoPhasesPostFilterFetchedBytes(TBase::GetDeriviative("TwoPhasesPostFilterFetchedBytes"))
    , TwoPhasesPostFilterUsefulBytes(TBase::GetDeriviative("TwoPhasesPostFilterUsefulBytes"))
    , HistogramCacheBlobsDuration(TBase::GetHistogram("CacheBlobsDurationMs", NMonitoring::ExponentialHistogram(12, 2)))
    , HistogramMissCacheBlobsDuration(TBase::GetHistogram("MissCacheBlobsDurationMs", NMonitoring::ExponentialHistogram(12, 2)))

{
}

std::shared_ptr<NKikimr::NColumnShard::TScanAggregations> TScanCounters::BuildAggregations() {
    return std::make_shared<TScanAggregations>(GetModuleId());
}

}
