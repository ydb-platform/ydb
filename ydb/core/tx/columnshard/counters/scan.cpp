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

    , NoScanRecords(TBase::GetDeriviative("NoScanRecords"))
    , NoScanIntervals(TBase::GetDeriviative("NoScanIntervals"))
    , LinearScanRecords(TBase::GetDeriviative("LinearScanRecords"))
    , LinearScanIntervals(TBase::GetDeriviative("LinearScanIntervals"))
    , LogScanRecords(TBase::GetDeriviative("LogScanRecords"))
    , LogScanIntervals(TBase::GetDeriviative("LogScanIntervals"))

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
    , BlobsReceivedBytes(TBase::GetDeriviative("BlobsReceivedBytes"))
{
    HistogramIntervalMemoryRequiredOnFail = TBase::GetHistogram("IntervalMemory/RequiredOnFail/Gb", NMonitoring::LinearHistogram(10, 1, 1));
    HistogramIntervalMemoryReduceSize = TBase::GetHistogram("IntervalMemory/Reduce/Gb", NMonitoring::ExponentialHistogram(8, 2, 1));
    HistogramIntervalMemoryRequiredAfterReduce = TBase::GetHistogram("IntervalMemory/RequiredAfterReduce/Mb", NMonitoring::ExponentialHistogram(10, 2, 64));
/*
    {
        const std::map<i64, TString> borders = {{0, "0"}, {512LLU * 1024 * 1024, "0.5Gb"}, {1LLU * 1024 * 1024 * 1024, "1Gb"},
            {2LLU * 1024 * 1024 * 1024, "2Gb"}, {3LLU * 1024 * 1024 * 1024, "3Gb"},
            {4LLU * 1024 * 1024 * 1024, "4Gb"}, {5LLU * 1024 * 1024 * 1024, "5Gb"},
            {6LLU * 1024 * 1024 * 1024, "6Gb"}, {7LLU * 1024 * 1024 * 1024, "7Gb"}, {8LLU * 1024 * 1024 * 1024, "8Gb"}};
        HistogramIntervalMemoryRequiredOnFail = std::make_shared<TDeriviativeHistogram>(module, "IntervalMemory/RequiredOnFail/Bytes", "", borders);
    }
    {
        const std::map<i64, TString> borders = {{0, "0"}, {512LLU * 1024 * 1024, "0.5Gb"}, {1LLU * 1024 * 1024 * 1024, "1Gb"},
            {2LLU * 1024 * 1024 * 1024, "2Gb"},
            {4LLU * 1024 * 1024 * 1024, "4Gb"},
            {8LLU * 1024 * 1024 * 1024, "8Gb"}, {16LLU * 1024 * 1024 * 1024, "16Gb"}, {32LLU * 1024 * 1024 * 1024, "32Gb"}};
        HistogramIntervalMemoryReduceSize = std::make_shared<TDeriviativeHistogram>(module, "IntervalMemory/Reduce/Bytes", "", borders);
    }
    {
        const std::map<i64, TString> borders = {{0, "0"}, {64LLU * 1024 * 1024, "64Mb"}, 
            {128LLU * 1024 * 1024, "128Mb"}, {256LLU * 1024 * 1024, "256Mb"}, {512LLU * 1024 * 1024, "512Mb"}, 
            {1024LLU * 1024 * 1024, "1024Mb"}, {2LLU * 1024 * 1024 * 1024, "2Gb"}, {3LLU * 1024 * 1024 * 1024, "3Gb"}
            };
        HistogramIntervalMemoryRequiredAfterReduce = std::make_shared<TDeriviativeHistogram>(module, "IntervalMemory/RequiredAfterReduce/Bytes", "", borders);
    }
*/
    ScanIntervalState = std::make_shared<TScanIntervalState>(*this);
    ResourcesSubscriberCounters = std::make_shared<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>();
    ScanDurationByStatus.resize((ui32)EStatusFinish::COUNT);
    ScansFinishedByStatus.resize((ui32)EStatusFinish::COUNT);
    ui32 idx = 0;
    for (auto&& i : GetEnumAllValues<EStatusFinish>()) {
        if (i == EStatusFinish::COUNT) {
            continue;
        }
        ScanDurationByStatus[(ui32)i] = TBase::GetHistogram("ScanDuration/" + ::ToString(i) + "/Milliseconds", NMonitoring::ExponentialHistogram(18, 2, 1));
        ScansFinishedByStatus[(ui32)i] = TBase::GetDeriviative("ScansFinised/" + ::ToString(i));
        AFL_VERIFY(idx == (ui32)i);
        ++idx;
    }
}

NKikimr::NColumnShard::TScanAggregations TScanCounters::BuildAggregations() {
    return TScanAggregations(GetModuleId());
}

void TScanCounters::FillStats(::NKikimrTableStats::TTableStats& output) const {
    output.SetRangeReads(ScansFinishedByStatus[(ui32)EStatusFinish::Success]->Val());
}

}
