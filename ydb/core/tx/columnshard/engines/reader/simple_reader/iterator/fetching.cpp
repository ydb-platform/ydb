#include "fetching.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/events.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

void TPredicateFilter::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step, const ui32 filteredRows) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(PredicateFilter, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), filteredRows, source->GetReservedMemory());
}

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source->GetStageData().GetTable().ToGeneralContainer(source->GetContext()->GetCommonContext()->GetResolver(),
            source->GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                source->GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()),
            true));
    const ui32 filteredRows = filter.GetFilteredCount().value_or(source->GetRecordsCount());
    source->MutableStageData().AddFilter(filter);
    ReportTracing(source, step, filteredRows);
    return true;
}

void VerifyConflictingPortion(const std::shared_ptr<NCommon::IDataSource>& source) {
    // the portion must be a simple portion
    AFL_VERIFY(source->GetType() == IDataSource::EType::SimplePortion);
    auto* portionSource = static_cast<TPortionDataSource*>(source.get());
    auto& info = portionSource->GetPortionInfo();
    auto status = portionSource->GetContext()->GetPortionStateAtScanStart(info);

    // let's check that the portion state is ok
    // we may have here only written portions (not compacted)
    AFL_VERIFY(info.GetPortionType() == EPortionType::Written);
    const auto& wPortionInfo = static_cast<const TWrittenPortionInfo&>(info);
    // we may have here only conflicting portions
    AFL_VERIFY(status.Conflicting);
    const auto& requestSnapshot = source->GetContext()->GetReadMetadata()->GetRequestSnapshot();
    // if portion was already committed at the scan start, it must have commit snapshot greater than the request snapshot
    if (status.Committed) {
        AFL_VERIFY(wPortionInfo.GetCommitSnapshotVerified() > requestSnapshot)("error", "portion was committed and conflicting at the scan start, but has commit snapshot less than the request snapshot")("portion_info", wPortionInfo.DebugString())("request_snapshot", requestSnapshot.DebugString());
    } else {
        // if the portion was uncommitted it means now it may be:
        // 1. still uncommitted
        if (!wPortionInfo.IsCommitted()) {
            // do nothing, it is just fine
        // 2. committed and removed, in this case its snapshot must be greater or equal to the request snapshot
        } else if (wPortionInfo.HasRemoveSnapshot()) {
            AFL_VERIFY(wPortionInfo.GetCommitSnapshotVerified() >= requestSnapshot)("error", "portion was uncommitted and conflicting at the scan start, but now it is removed and committed and has commit snapshot less than the request snapshot")("portion_info", wPortionInfo.DebugString())("request_snapshot", requestSnapshot.DebugString());
        // 3. committed and not removed, in this case its snapshot must be greater than the request snapshot
        } else {
            AFL_VERIFY(wPortionInfo.GetCommitSnapshotVerified() > requestSnapshot)("error", "portion was uncommitted and conflicting at the scan start, but now it is committed and has commit snapshot less than the request snapshot")("portion_info", wPortionInfo.DebugString())("request_snapshot", requestSnapshot.DebugString());
        }
    }
    // source must not be empty, we will mark it as conflicting
    AFL_VERIFY(source->GetRecordsCount() > 0)("error", "source has no records");
}

void TConflictDetector::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(ConflictDetector, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TConflictDetector::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    VerifyConflictingPortion(source);
    // it is not empty (not filtered everything out by other filters) and conflicting, so we must mark the conflict here
    AFL_VERIFY(source->AddTxConflict());
    ReportTracing(source, step);
    return true;
}

void TSnapshotFilter::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(SnapshotFilter, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    auto filter =
        MakeSnapshotFilter(source->GetStageData().GetTable().ToTable(
                               std::set<ui32>({ (ui32)IIndexInfo::ESpecialColumn::PLAN_STEP, (ui32)IIndexInfo::ESpecialColumn::TX_ID }),
                               source->GetContext()->GetCommonContext()->GetResolver()),
            source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source->GetRecordsCount()) != source->GetRecordsCount()) {
        if (source->AddTxConflict()) {
            ReportTracing(source, step);
            return true;
        }
    }
    source->MutableStageData().AddFilter(filter);
    ReportTracing(source, step);
    return true;
}

void TDeletionFilter::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(DeletionFilter, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TDeletionFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    if (!source->GetStageData().GetTable().HasColumn((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
        ReportTracing(source, step);
        return true;
    }
    auto filterTable = source->GetStageData().GetTable().ToTable(std::set<ui32>({ (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG }));
    if (!filterTable) {
        ReportTracing(source, step);
        return true;
    }
    AFL_VERIFY(filterTable->column(0)->type()->id() == arrow::boolean()->id());
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& i : filterTable->column(0)->chunks()) {
        auto filterFlags = static_pointer_cast<arrow::BooleanArray>(i);
        for (ui32 i = 0; i < filterFlags->length(); ++i) {
            filter.Add(!filterFlags->GetView(i));
        }
    }
    source->MutableStageData().AddFilter(filter);
    ReportTracing(source, step);
    return true;
}

void TShardingFilter::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(ShardingFilter, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TShardingFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source->GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    const std::set<ui32> ids = source->GetContext()->GetCommonContext()->GetResolver()->GetColumnIdsSetVerified(shardingInfo->GetColumnNames());
    auto filter =
        shardingInfo->GetFilter(source->GetStageData().GetTable().ToTable(ids, source->GetContext()->GetCommonContext()->GetResolver()));
    source->MutableStageData().AddFilter(filter);
    ReportTracing(source, step);
    return true;
}

void TFilterCutLimit::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(FilterCutLimit, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

NKikimr::TConclusion<bool> TFilterCutLimit::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    source->MutableStageData().CutFilter(source->GetRecordsCount(), Limit, Reverse);
    ReportTracing(source, step);
    return true;
}

void TDetectInMemFlag::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step, const ui64 columnRawBytes, const ui64 columnBlobBytes) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(DetectInMemFlag, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, columnBlobBytes, columnRawBytes, source->IsSourceInMemory(), source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TDetectInMemFlag::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    if (!source->NeedPortionData()) {
        source->SetSourceInMemory(true);
        source->MutableAs<IDataSource>()->InitUsedRawBytes();
    }
    if (source->HasSourceInMemoryFlag()) {
        ReportTracing(source, step, 0UL, 0UL);
        return true;
    }
    ui64 columnRawBytes = 0;
    ui64 columnBlobBytes = 0;

    const auto& program = source->GetContext()->GetReadMetadata()->GetProgram();
    // hasAggregations is false if there's no graph (GetGraphOptional returns nullptr)
    const bool hasAggregations = program.GetGraphOptional() && program.GetChainVerified()->HasAggregations();
    if (Columns.GetColumnsCount() && !hasAggregations) {
        columnRawBytes = source->GetColumnRawBytes(Columns.GetColumnIds());
        columnBlobBytes = source->GetColumnBlobBytes(Columns.GetColumnIds());

        source->SetSourceInMemory(
            columnRawBytes < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source->SetSourceInMemory(true);
    }
    ReportTracing(source, step, columnRawBytes, columnBlobBytes);
    return true;
}

namespace {
class TApplySourceResult: public IApplyAction {
private:
    using TBase = IDataTasksProcessor::ITask;
    YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
    TFetchingScriptCursor Step;

public:
    TApplySourceResult(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step)
        : Source(source)
        , Step(step) {
    }

    virtual ui64 GetSourceId() const override {
        return Source ? Source->GetDeprecatedPortionId() : 0;
    }

    virtual bool DoApply(IDataReader& indexedDataRead) override {
        auto* plainReader = static_cast<TPlainReadData*>(&indexedDataRead);
        Source->MutableAs<IDataSource>()->SetCursor(std::move(Step));
        Source->StartSyncSection();
        const ui32 syncPointIndex = Source->GetAs<IDataSource>()->GetPurposeSyncPointIndex();
        plainReader->MutableScanner().GetSyncPoint(syncPointIndex)->OnSourcePrepared(std::move(Source), *plainReader);
        return true;
    }
};

}   // namespace

void TUpdateAggregatedMemoryStep::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(UpdateAggregatedMemory, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TUpdateAggregatedMemoryStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    if (auto* portionSource = source->MutableOptionalAs<TPortionDataSource>()) {
        portionSource->ActualizeAggregatedMemoryGuards();
    }
    ReportTracing(source, step);
    return true;
}

void TInitializeSourceStep::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(InitializeSource, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TInitializeSourceStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    auto* simpleSource = source->MutableAs<IDataSource>();
    simpleSource->InitializeProcessing(source);
    ReportTracing(source, step);
    return true;
}

void TPortionAccessorFetchedStep::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(PortionAccessorFetched, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());

TConclusion<bool> TDecideStreamingModeStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    // Only applies to non-in-memory portion sources.
    if (source->IsSourceInMemory()) {
        return true;
    }
    if (!source->HasPortionAccessor()) {
        // Accessor not yet available (should not happen in normal flow, but be safe).
        return true;
    }

    auto* simpleSource = source->MutableAs<IDataSource>();
    if (simpleSource->HasEarlyPages()) {
        // Already decided (e.g. re-entry after ContinueCursor for next page).
        // The page index was already advanced by ContinueCursor before restarting.
        return true;
    }

    const ui64 memoryLimit = AppDataVerified().ColumnShardConfig.GetMemoryLimitScanPortion();
    if (memoryLimit == 0) {
        // No memory limit configured — streaming mode is not applicable.
        return true;
    }

    auto pages = source->GetPortionAccessor().BuildReadPages(
        memoryLimit, source->GetContext()->GetProgramInputColumns()->GetColumnIds());

    const bool streamingMode = TStreamingConfigHelper::ShouldUseStreamingMode();

    simpleSource->SetEarlyPages(std::move(pages), streamingMode);

    if (streamingMode) {
        // Store the fetch script so ContinueCursor can restart from the beginning
        // for each subsequent page without re-running TDecideStreamingModeStep's
        // expensive BuildReadPages call (HasEarlyPages() guards that).
        simpleSource->SetStreamingFetchScript(step.GetScript());
    }
    return true;
}

TConclusion<bool> TPortionAccessorFetchedStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    source->MutableAs<IDataSource>()->InitUsedRawBytes();
    ReportTracing(source, step);
    return true;
}

void TStepAggregationSources::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(AggregationSources, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TStepAggregationSources::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    AFL_VERIFY(source->GetType() == IDataSource::EType::SimpleAggregation);
    auto* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
    std::vector<std::unique_ptr<NArrow::NSSA::TAccessorsCollection>> collections;
    for (auto&& i : aggrSource->GetSources()) {
        collections.emplace_back(i->MutableStageData().ExtractTable());
    }
    auto conclusion = Aggregator->Execute(std::move(collections), source->MutableStageData().MutableTable());
    if (conclusion.IsFail()) {
        return conclusion;
    }
    source->BuildStageResult(source);
    ReportTracing(source, step);
    return true;
}

void TCleanAggregationSources::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(CleanAggregationSources, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

TConclusion<bool> TCleanAggregationSources::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    AFL_VERIFY(source->GetType() == IDataSource::EType::SimpleAggregation);
    auto* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
    for (auto&& i : aggrSource->GetSources()) {
        i->MutableAs<IDataSource>()->ClearResult();
    }
    ReportTracing(source, step);
    return true;
}

bool TBuildResultStep::IsPageSkippedByFilter(const std::shared_ptr<NCommon::IDataSource>& source) const {
    const auto& notAppliedFilter = source->GetStageResult().GetNotAppliedFilter();
    if (notAppliedFilter && !notAppliedFilter->IsTotalAllowFilter()) {
        const auto pageFilter = notAppliedFilter->Slice(StartIndex, RecordsCount);
        return pageFilter.IsTotalDenyFilter();
    }
    return false;
}

void TBuildResultStep::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step,
    const TDuration executionDurationMs) const {
    if (!LWPROBE_ENABLED(BuildResult) && !NLWTrace::HasShuttles(source->GetDataSourceOrbit())) {
        return;
    }
    const TDuration durationMs = source->GetAndResetWaitDuration();
    ui32 pageFilteredRowsCount = RecordsCount;
    const auto& notAppliedFilter = source->GetStageResult().GetNotAppliedFilter();
    if (notAppliedFilter && !notAppliedFilter->IsTotalAllowFilter()) {
        const auto pageFilter = notAppliedFilter->Slice(StartIndex, RecordsCount);
        pageFilteredRowsCount = pageFilter.GetFilteredCount().value_or(RecordsCount);
    }
    LWTRACK(BuildResult, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, executionDurationMs, pageFilteredRowsCount, RecordsCount, source->GetReservedMemory(),
            source->GetSourcesAheadQueueWaitDuration(), source->GetSourcesAhead());
}

std::shared_ptr<arrow::Table> TBuildResultStep::BuildPageResultBatch(const std::shared_ptr<NCommon::IDataSource>& source) const {
    if (IsPageSkippedByFilter(source)) {
        return nullptr;
    }
    auto context = source->GetContext();
    NArrow::TGeneralContainer::TTableConstructionContext contextTableConstruct;
    if (!source->IsSourceInMemory()) {
        // Determine the correct start index for slicing the assembled batch:
        //
        // Old path (DoAssembleColumns with PrepareForAssemblePageImpl):
        //   The assembled batch contains only the current page's rows, starting at row 0
        //   (page-relative).  IsAssembledDataPageRelative() returns true.
        //   → batchStartIndex = 0
        //
        // NSSA path (TProgramStep → DoStartFetchData → DoAssembleAccessor):
        //   The assembled batch contains the entire portion's rows, starting at row 0
        //   of the portion (portion-relative).  IsAssembledDataPageRelative() returns false.
        //   → batchStartIndex = StartIndex (absolute page start within the portion)
        const auto* simpleSource = source->GetAs<IDataSource>();
        const bool isStreamingPerPage = simpleSource->IsStreamingMode() && simpleSource->HasEarlyPages();
        const bool pageRelative = isStreamingPerPage && simpleSource->IsAssembledDataPageRelative();
        const ui32 batchStartIndex = pageRelative ? 0 : StartIndex;
        contextTableConstruct.SetStartIndex(batchStartIndex).SetRecordsCount(RecordsCount);
    } else {
        AFL_VERIFY(StartIndex == 0);
        AFL_VERIFY(RecordsCount == source->GetRecordsCount())("records_count", RecordsCount)("source", source->GetRecordsCount());
    }
    contextTableConstruct.SetFilter(source->GetStageResult().GetNotAppliedFilter());
    if (source->GetStageResult().IsEmpty()) {
        return nullptr;
    }
    auto resultBatch = source->GetStageResult().GetBatch()->BuildTableVerified(contextTableConstruct);
    return resultBatch->num_rows() ? resultBatch : nullptr;
}

TConclusion<bool> TBuildResultStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TMonotonic startExecution = TMonotonic::Now();
    auto context = source->GetContext();
    auto resultBatch = BuildPageResultBatch(source);
    auto* sSource = source->MutableAs<IDataSource>();
    const ui32 recordsCount = resultBatch ? resultBatch->num_rows() : 0;
    context->GetCommonContext()->GetCounters().OnSourceFinished(source->GetRecordsCount(), sSource->GetUsedRawBytes(), recordsCount);
    sSource->MutableResultRecordsCount() += recordsCount;
    source->MutableStageResult().SetResultChunk(std::move(resultBatch), StartIndex, RecordsCount);
    ReportTracing(source, step, TMonotonic::Now() - startExecution);
    const ui64 blobBytes = source->GetTotalBytesRead();
    NActors::TActivationContext::AsActorContext().Send(context->GetCommonContext()->GetScanActorId(),
        new NColumnShard::TEvPrivate::TEvTaskProcessedResult(std::make_shared<TApplySourceResult>(source, step),
            source->GetContext()->GetCommonContext()->GetCounters().GetResultsForSourceGuard(), source->GetDeprecatedPortionId(),
            blobBytes, sSource->GetUsedRawBytes(), recordsCount, source->GetRecordsCount(),
            source->GetReservedMemory()));
    return false;
}

void TPrepareResultStep::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step,
    const TDuration executionDurationMs) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(PrepareResult, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, executionDurationMs, source->GetFilteredRowsCount(), source->GetReservedMemory(),
            source->GetSourcesAheadQueueWaitDuration(), source->GetSourcesAhead());
}

TConclusion<bool> TPrepareResultStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TMonotonic startExecution = TMonotonic::Now();
    const auto context = source->GetContext();
    NCommon::TFetchingScriptBuilder acc(*context);
    if (source->IsSourceInMemory()) {
        AFL_VERIFY(source->GetStageResult().GetPagesToResultVerified().size() == 1);
    }
    AFL_VERIFY(!source->GetStageResult().IsEmpty());
    auto* sSource = source->MutableAs<IDataSource>();
    for (auto&& i : source->GetStageResult().GetPagesToResultVerified()) {
        if (sSource->GetIsStartedByCursor() && !context->GetCommonContext()->GetScanCursor()->CheckSourceIntervalUsage(
                                                  source->GetSourceIdx(), i.GetIndexStart(), i.GetRecordsCount())) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TPrepareResultStep_ResultStep_SKIP_CURSOR")(
                "source_idx", source->GetSourceIdx());
            source->MutableStageResult().ExtractPageForResult();
            continue;
        }
        acc.AddStep(std::make_shared<TBuildResultStep>(i.GetIndexStart(), i.GetRecordsCount()));
    }
    auto plan = std::move(acc).Build();
    AFL_VERIFY(!plan->IsFinished(0));
    source->MutableAs<IDataSource>()->InitFetchingPlan(plan);
    ReportTracing(source, step, TMonotonic::Now() - startExecution);
    if (StartResultBuildingInplace) {
        TFetchingScriptCursor cursor(plan, 0);
        return cursor.Execute(source);
    } else {
        return true;
    }
}

void TDuplicateFilter::TFilterSubscriber::ReportTracing(const std::shared_ptr<NCommon::IDataSource>& source) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(Deduplication, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), Step.GetStepIndex(),
            Step.GetTracingName(), durationMs, source->GetRecordsCount(), source->GetReservedMemory());
}

void TDuplicateFilter::TFilterSubscriber::OnFilterReady(NArrow::TColumnFilter&& filter) {
    if (auto source = Source.lock()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetch_filter")("source", source->GetSourceIdx())(
            "filter", filter.DebugString())("aborted", source->GetContext()->IsAborted());
        if (source->GetContext()->IsAborted()) {
            return;
        }
        AFL_VERIFY(filter.GetRecordsCountVerified() == source->GetRecordsCount())("filter", filter.GetRecordsCountVerified())(
                                                         "source", source->GetRecordsCount());

        ReportTracing(source);

        if (const std::shared_ptr<NArrow::TColumnFilter> appliedFilter = source->GetStageData().GetAppliedFilter()) {
            filter = filter.ApplyFilterFrom(*appliedFilter);
        }
        source->MutableStageData().AddFilter(std::move(filter));
        Step.Next();
        const auto convActorId = source->GetContext()->GetCommonContext()->GetConveyorProcessId();
        const auto scanActorId = source->GetContext()->GetCommonContext()->GetScanActorId();
        auto task = std::make_shared<TStepAction>(std::move(source), std::move(Step), scanActorId, false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, convActorId);
    }
}

void TDuplicateFilter::TFilterSubscriber::OnFailure(const TString& reason) {
    if (auto source = Source.lock()) {
        source->GetContext()->GetCommonContext()->AbortWithError("cannot build duplicate filter: " + reason);
    }
}

TDuplicateFilter::TFilterSubscriber::TFilterSubscriber(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step)
    : Source(source)
    , Step(step)
    , TaskGuard(source->GetContext()->GetCommonContext()->GetCounters().GetFilterFetchingGuard()) {
}

TConclusion<bool> TDuplicateFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    source->MutableAs<TPortionDataSource>()->StartFetchingDuplicateFilter(std::make_shared<TFilterSubscriber>(source, step));
    return false;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
