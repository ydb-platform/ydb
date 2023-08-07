#include "scan.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module)
    : TBase(module)
    , ProcessingOverload(TBase::GetDeriviative("ProcessingOverload"))
    , ReadingOverload(TBase::GetDeriviative("ReadingOverload"))
    , PriorityFetchBytes(TBase::GetDeriviative("PriorityFetch/Bytes"))
    , PriorityFetchCount(TBase::GetDeriviative("PriorityFetch/Count"))
    , GeneralFetchBytes(TBase::GetDeriviative("GeneralFetch/Bytes"))
    , GeneralFetchCount(TBase::GetDeriviative("GeneralFetch/Count"))
    , HasResultsAckRequest(TBase::GetDeriviative("HasResultsAckRequest"))
    , NoResultsAckRequest(TBase::GetDeriviative("NoResultsAckRequest"))
    , AckWaitingDuration(TBase::GetDeriviative("AckWaitingDuration"))
    , ScanDuration(TBase::GetDeriviative("ScanDuration"))

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

    , Hanging(TBase::GetDeriviative("Hanging"))

    , HistogramCacheBlobsCountDuration(TBase::GetHistogram("CacheBlobsCountDurationMs", NMonitoring::ExponentialHistogram(18, 2, 8)))
    , HistogramMissCacheBlobsCountDuration(TBase::GetHistogram("MissCacheBlobsCountDurationMs", NMonitoring::ExponentialHistogram(18, 2, 8)))
    , HistogramCacheBlobBytesDuration(TBase::GetHistogram("CacheBlobBytesDurationMs", NMonitoring::ExponentialHistogram(18, 2, 8)))
    , HistogramMissCacheBlobBytesDuration(TBase::GetHistogram("MissCacheBlobBytesDurationMs", NMonitoring::ExponentialHistogram(18, 2, 8)))

    , BlobsWaitingDuration(TBase::GetDeriviative("BlobsWaitingDuration"))
    , HistogramBlobsWaitingDuration(TBase::GetHistogram("BlobsWaitingDuration", NMonitoring::ExponentialHistogram(20, 2, 256)))

    , BlobsReceivedCount(TBase::GetDeriviative("BlobsReceivedCount"))
    , BlobsReceivedBytes(TBase::GetDeriviative("BlobsReceivedBytes")) {
}

NKikimr::NColumnShard::TScanAggregations TScanCounters::BuildAggregations() {
    return TScanAggregations(GetModuleId());
}

}
