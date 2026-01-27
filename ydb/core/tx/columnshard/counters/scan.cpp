#include "scan.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>

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
    , NoIndexBlobs(TBase::GetDeriviative("Indexes/NoData/Blobs/Count"))
    , NoIndex(TBase::GetDeriviative("Indexes/NoData/Index/Count"))
    , RecordsAcceptedByIndex(TBase::GetDeriviative("Indexes/Accepted/Records"))
    , RecordsDeniedByIndex(TBase::GetDeriviative("Indexes/Denied/Records"))
    , RecordsAcceptedByHeader(TBase::GetDeriviative("Headers/Accepted/Records"))
    , RecordsDeniedByHeader(TBase::GetDeriviative("Headers/Denied/Records"))
    , HangingRequests(TBase::GetDeriviative("HangingRequests"))

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
    , ProcessedSourceCount(TBase::GetDeriviative("ProcessedSource/Count"))
    , ProcessedSourceRawBytes(TBase::GetDeriviative("ProcessedSource/RawBytes"))
    , ProcessedSourceRecords(TBase::GetDeriviative("ProcessedSource/Records"))
    , ProcessedSourceEmptyCount(TBase::GetDeriviative("ProcessedSource/Empty/Count"))
    , HistogramFilteredResultCount(TBase::GetHistogram("ProcessedSource/Filtered/Count", NMonitoring::ExponentialHistogram(20, 2)))
{
    SubColumnCounters = std::make_shared<TSubColumnCounters>(CreateSubGroup("Speciality", "SubColumns"));
    DuplicateFilteringCounters = std::make_shared<TDuplicateFilteringCounters>();

    HistogramIntervalMemoryRequiredOnFail = TBase::GetHistogram("IntervalMemory/RequiredOnFail/Gb", NMonitoring::LinearHistogram(10, 1, 1));
    HistogramIntervalMemoryReduceSize = TBase::GetHistogram("IntervalMemory/Reduce/Gb", NMonitoring::ExponentialHistogram(8, 2, 1));
    HistogramIntervalMemoryRequiredAfterReduce =
        TBase::GetHistogram("IntervalMemory/RequiredAfterReduce/Mb", NMonitoring::ExponentialHistogram(10, 2, 64));
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
        ScanDurationByStatus[(ui32)i] =
            TBase::GetHistogram("ScanDuration/" + ::ToString(i) + "/Milliseconds", NMonitoring::ExponentialHistogram(18, 2, 1));
        ScansFinishedByStatus[(ui32)i] = TBase::GetDeriviative("ScansFinished/" + ::ToString(i));
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

TConcreteScanCounters::TConcreteScanCounters(
    const TScanCounters& counters, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program)
    : TBase(counters)
    , Aggregations(TBase::BuildAggregations())
{
    if (program) {
        for (auto&& i : program->GetNodes()) {
            SkipNodesCount.emplace(i.first, std::make_shared<TAtomicCounter>());
            ExecuteNodesCount.emplace(i.first, std::make_shared<TAtomicCounter>());
        }
    }
}

TString TConcreteScanCounters::TPerStepCounters::DebugString() const {
    return TStringBuilder() << "ExecutionDuration:" << NKqp::FormatDurationAsMilliseconds(ExecutionDuration) << ";"
                            << "WaitDuration:" << NKqp::FormatDurationAsMilliseconds(WaitDuration) << ";"
                            << "RawBytesRead:" << RawBytesRead;
}

THashMap<TString, TConcreteScanCounters::TPerStepCounters> TConcreteScanCounters::ReadStepsCounters() const {
    THashMap<TString, TPerStepCounters> counters;
    auto lock = AtomicStepCounters.ReadGuard();
    for (auto& [k, v] : lock.Value) {
        TPerStepCounters thisCounters;
        thisCounters.ExecutionDuration = TDuration::MicroSeconds(v.ExecutionDurationMicroSeconds->Val());
        thisCounters.WaitDuration = TDuration::MicroSeconds(v.WaitDurationMicroSeconds->Val());
        thisCounters.RawBytesRead = v.RawBytesRead->Val();
        counters[k] = thisCounters;
    }
    return counters;
}

TString TConcreteScanCounters::StepsCountersDebugString() const {
    auto counters = ReadStepsCounters();
    TPerStepCounters summ;
    TStringBuilder bld;
    bld << "per_step_counters:(";
    for (auto& [k, v] : counters) {
        summ.ExecutionDuration += v.ExecutionDuration;
        summ.WaitDuration += v.WaitDuration;
        summ.RawBytesRead += v.RawBytesRead;
        bld << "[StepName: " << k << "; " << v.DebugString() << "],\n";
    }
    if (!bld.empty()) {
        bld.pop_back();   // \n
        bld.pop_back();   // ,
    }
    bld << ");";
    bld << "counters_summ_across_all_steps:(";
    bld << "[StepName: AllSteps; " << summ.DebugString() << "])\n";
    return bld;
}

TConcreteScanCounters::TPerStepAtomicCounters TConcreteScanCounters::CountersForStep(TStringBuf stepName) const {
    auto* counterIfExists = [&] {
        auto lock = AtomicStepCounters.ReadGuard();
        return lock.Value.FindPtr(stepName);
    }();
    if (counterIfExists) [[likely]] {
        return *counterIfExists;
    }
    auto lock = AtomicStepCounters.WriteGuard();
    auto [it, ok] = lock.Value.emplace(stepName, TPerStepAtomicCounters{});
    return it->second;
}

}   // namespace NKikimr::NColumnShard
