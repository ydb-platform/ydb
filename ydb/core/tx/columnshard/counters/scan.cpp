#include "scan.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module)
    : TBase(module)
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
    , DictionaryOnlyOptimizationCount(TBase::GetDeriviative("Dictionary/OnlyOptimization/Count"))
    , DistinctLimitSyncPointInvocations(TBase::GetDeriviative("DistinctLimit/SyncPoint/Invocations"))
    , PredicateFilterInvocations(TBase::GetDeriviative("PredicateFilter/Invocations"))
    , EarlyInFlightReleaseCount(TBase::GetDeriviative("EarlyInFlightRelease/Count"))
    , HangingRequests(TBase::GetDeriviative("HangingRequests"))

    , HistogramReadMetadataDurationMs(TBase::GetHistogram("Portions/ReadMetadataDurationMs", NMonitoring::ExponentialHistogram(15, 2, 8)))

    , ProcessedSourceCount(TBase::GetDeriviative("ProcessedSource/Count"))
    , ProcessedSourceRawBytes(TBase::GetDeriviative("ProcessedSource/RawBytes"))
    , ProcessedSourceRecords(TBase::GetDeriviative("ProcessedSource/Records"))
    , ProcessedSourceEmptyCount(TBase::GetDeriviative("ProcessedSource/Empty/Count"))
    , StartedSourceConflictingCount(TBase::GetDeriviative("StartedSource/Conflicting/Count"))
    , StartedSourceNonconflictingCount(TBase::GetDeriviative("StartedSource/Nonconflicting/Count"))
    , HistogramFilteredResultCount(TBase::GetHistogram("ProcessedSource/Filtered/Count", NMonitoring::ExponentialHistogram(20, 2)))
{
    SubColumnCounters = std::make_shared<TSubColumnCounters>(CreateSubGroup("Speciality", "SubColumns"));
    DuplicateFilteringCounters = std::make_shared<TDuplicateFilteringCounters>();
    SimpleDuplicateFilteringCounters = std::make_shared<TSimpleDuplicateFilteringCounters>();

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

void TScanCounters::FillStats(::NKikimrTableStats::TTableStats& output) const {
    output.SetRangeReads(ScansFinishedByStatus[(ui32)EStatusFinish::Success]->Val());
}

TConcreteScanCounters::TConcreteScanCounters(
    const TScanCounters& counters, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program)
    : TBase(counters)
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
    TPerStepCounters sum;
    TStringBuilder bld;
    bld << "per_step_counters:(";
    for (auto& [k, v] : counters) {
        sum.ExecutionDuration += v.ExecutionDuration;
        sum.WaitDuration += v.WaitDuration;
        sum.RawBytesRead += v.RawBytesRead;
        bld << "[StepName: " << k << "; " << v.DebugString() << "],\n";
    }
    if (!counters.empty()) {
        bld.pop_back();   // \n
        bld.pop_back();   // ,
    }
    bld << ");";
    bld << "counters_summ_across_all_steps:(";
    bld << "[StepName: AllSteps; " << sum.DebugString() << "])\n";
    return bld;
}

TConcreteScanCounters::TPerStepAtomicCounters TConcreteScanCounters::CountersForStep(TStringBuf stepName) const {
    {
        auto lock = AtomicStepCounters.ReadGuard();
        auto* counterIfExists = lock.Value.FindPtr(stepName);
        if (counterIfExists) [[likely]] {
            return *counterIfExists;   // Copy made while lock is held
        }
    }
    auto lock = AtomicStepCounters.WriteGuard();
    auto [it, ok] = lock.Value.emplace(stepName, TPerStepAtomicCounters{});
    return it->second;
}

}   // namespace NKikimr::NColumnShard
