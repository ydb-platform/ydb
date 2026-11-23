#include "scan.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>

#include <algorithm>
#include <type_traits>

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
    , DictionaryOnlyOptimizationCount(TBase::GetDeriviative("Dictionary/OnlyOptimization/Count"))
    , DistinctLimitSyncPointInvocations(TBase::GetDeriviative("DistinctLimit/SyncPoint/Invocations"))
    , PredicateFilterInvocations(TBase::GetDeriviative("PredicateFilter/Invocations"))
    , EarlyInFlightReleaseCount(TBase::GetDeriviative("EarlyInFlightRelease/Count"))
    , HangingRequests(TBase::GetDeriviative("HangingRequests"))

    , HistogramReadMetadataDurationMs(TBase::GetHistogram("Portions/ReadMetadataDurationMs", NMonitoring::ExponentialHistogram(15, 2, 8)))

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
    , StartedSourceConflictingCount(TBase::GetDeriviative("StartedSource/Conflicting/Count"))
    , StartedSourceNonconflictingCount(TBase::GetDeriviative("StartedSource/Nonconflicting/Count"))
    , HistogramFilteredResultCount(TBase::GetHistogram("ProcessedSource/Filtered/Count", NMonitoring::ExponentialHistogram(20, 2)))
{
    SubColumnCounters = std::make_shared<TSubColumnCounters>(CreateSubGroup("Speciality", "SubColumns"));
    DuplicateFilteringCounters = std::make_shared<TDuplicateFilteringCounters>();
    SimpleDuplicateFilteringCounters = std::make_shared<TSimpleDuplicateFilteringCounters>();

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
        YDB_LOG_VERIFY(idx == (ui32)i);
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

namespace {

template <class T>
void AppendUnique(std::vector<T>& target, const std::vector<T>& source) {
    for (const auto& item : source) {
        if (std::find(target.begin(), target.end(), item) == target.end()) {
            target.push_back(item);
        }
    }
}

}   // namespace

void TReserveMemoryStepDetails::Merge(const TReserveMemoryStepDetails& other) {
    for (const auto& source : other.Stages) {
        const auto stage = std::find_if(Stages.begin(), Stages.end(),
            [&source](const TStage& current) { return source.Stage == current.Stage; });
        auto& target = stage == Stages.end() ? Stages.emplace_back(TStage{source.Stage, {}}) : *stage;
        AppendUnique(target.MemoryTypes, source.MemoryTypes);
    }
}

void TFetchColumnsStepDetails::Merge(const TFetchColumnsStepDetails& other) {
    AppendUnique(Columns, other.Columns);
}

void TAssembleStepDetails::Merge(const TAssembleStepDetails& other) {
    AppendUnique(Columns, other.Columns);
}

void TPKFilterDetails::Merge(const TPKFilterDetails& other) {
    AppendUnique(Columns, other.Columns);
    AppendUnique(Predicates, other.Predicates);
}

void TShardingFilterStepDetails::Merge(const TShardingFilterStepDetails& other) {
    AppendUnique(Columns, other.Columns);
}

void TDeletionFilterStepDetails::Merge(const TDeletionFilterStepDetails& other) {
    AppendUnique(Columns, other.Columns);
}

void TSnapshotFilterStepDetails::Merge(const TSnapshotFilterStepDetails& other) {
    AppendUnique(Snapshots, other.Snapshots);
}

void TLimitStepDetails::Merge(const TLimitStepDetails& other) {
    AppendUnique(Limits, other.Limits);
}

void TPrepareResultStepDetails::Merge(const TPrepareResultStepDetails& other) {
    AppendUnique(StartResultBuildingInplace, other.StartResultBuildingInplace);
}

void TBuildResultStepDetails::Merge(const TBuildResultStepDetails& other) {
    if (const auto& source = other.PageBounds) {
        auto& bounds = PageBounds ? *PageBounds : PageBounds.emplace(*source);
        bounds.MinStartIndex = std::min(bounds.MinStartIndex, source->MinStartIndex);
        bounds.MaxStartIndex = std::max(bounds.MaxStartIndex, source->MaxStartIndex);
        bounds.MinRecordsCount = std::min(bounds.MinRecordsCount, source->MinRecordsCount);
        bounds.MaxRecordsCount = std::max(bounds.MaxRecordsCount, source->MaxRecordsCount);
    }
}

std::vector<TConcreteScanCounters::TStepHandle> TConcreteScanCounters::RegisterSteps(
    const std::vector<TStepDescription>& descriptions) const {
    auto lock = FetchingStepCounters->WriteGuard();
    auto& steps = lock.Value;
    const auto mergeDetails = [](TStepDetails& target, const TStepDetails& source) {
        std::visit(
            [&target](const auto& details) {
                using TDetails = std::decay_t<decltype(details)>;
                if constexpr (!std::is_same_v<TDetails, std::monostate>) {
                    auto* current = std::get_if<TDetails>(&target);
                    auto& targetDetails = current ? *current : target.emplace<TDetails>();
                    targetDetails.Merge(details);
                }
            },
            source);
    };
    std::vector<TStepList::iterator> existing;
    existing.reserve(steps.size());
    for (auto it = steps.begin(); it != steps.end(); ++it) {
        existing.push_back(it);
    }

    // Compute the longest common subsequence of each pair of suffixes.
    std::vector<std::vector<size_t>> matches(descriptions.size() + 1, std::vector<size_t>(existing.size() + 1));
    for (size_t i = descriptions.size(); i != 0; --i) {
        for (size_t j = existing.size(); j != 0; --j) {
            if (descriptions[i - 1].Name == existing[j - 1]->Name) {
                matches[i - 1][j - 1] = 1 + matches[i][j];
            } else {
                matches[i - 1][j - 1] = std::max(matches[i][j - 1], matches[i - 1][j]);
            }
        }
    }

    std::vector<TStepHandle> handles;
    handles.reserve(descriptions.size());
    size_t i = 0;
    size_t j = 0;
    while (i < descriptions.size()) {
        if (j < existing.size() && descriptions[i].Name == existing[j]->Name) {
            Y_ABORT_UNLESS(descriptions[i].Details.index() == existing[j]->Details.index(),
                "Inconsistent details type for step %s", descriptions[i].Name.c_str());
            mergeDetails(existing[j]->Details, descriptions[i].Details);
            handles.push_back(TStepHandle(existing[j]));
            ++i;
            ++j;
        } else if (j < existing.size() && matches[i][j + 1] > matches[i + 1][j]) {
            ++j;
        } else {
            // On a tie, keep the earlier existing position available for a later match.
            const auto next = j < existing.size() ? existing[j] : steps.end();
            TStepCounters step{descriptions[i].Name, {}, {}};
            mergeDetails(step.Details, descriptions[i].Details);
            handles.push_back(TStepHandle(steps.insert(next, std::move(step))));
            ++i;
        }
    }
    return handles;
}

namespace {

void RecordStepMeasurement(TStepMeasurementCounters& counter, std::optional<ui64> value) {
    if (value) {
        counter.Sum += *value;
        ++counter.Count;
    }
}

}   // namespace

void TConcreteScanCounters::RecordStepExecution(TStepHandle handle, TDuration activeTime, std::optional<ui64> inputRows,
    std::optional<ui64> outputRows, std::optional<ui64> inputBytes, std::optional<ui64> outputBytes) const {
    auto lock = FetchingStepCounters->WriteGuard();
    auto& statistics = handle.Position->Statistics;
    ++statistics.Calls;
    statistics.ActiveTime += activeTime;
    RecordStepMeasurement(statistics.InputRows, inputRows);
    RecordStepMeasurement(statistics.OutputRows, outputRows);
    RecordStepMeasurement(statistics.InputBytes, inputBytes);
    RecordStepMeasurement(statistics.OutputBytes, outputBytes);
}

void TConcreteScanCounters::AddStepWait(TStepHandle handle, TDuration waitTime) const {
    auto lock = FetchingStepCounters->WriteGuard();
    handle.Position->Statistics.WaitTime += waitTime;
}

void TConcreteScanCounters::AddStepReadBytes(TStepHandle handle, ui64 readBytes) const {
    auto lock = FetchingStepCounters->WriteGuard();
    RecordStepMeasurement(handle.Position->Statistics.ReadBytes, readBytes);
}

std::vector<TStepCounters> TConcreteScanCounters::GetStepStats() const {
    auto lock = FetchingStepCounters->ReadGuard();
    return std::vector<TStepCounters>(lock.Value.begin(), lock.Value.end());
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
