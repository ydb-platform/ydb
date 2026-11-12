#include "fetching.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/duplicates/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NTrivial {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

void TPredicateFilter::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step, const ui32 filteredRows) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(PredicateFilter, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), filteredRows, source.GetReservedMemory());
}

TConclusion<TExecutionResult> TPredicateFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    auto filter = source.GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source.GetStageData().GetTable().ToGeneralContainer(source.GetContext()->GetCommonContext()->GetResolver(),
            source.GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                source.GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()), true));
    const ui32 filteredRows = filter.GetFilteredCount().value_or(source.GetRecordsCount());
    source.MutableStageData().AddFilter(filter);
    source.GetContext()->GetCommonContext()->GetCounters().OnPredicateFilterInvocation();
    ReportTracing(source, step, filteredRows);
    return TExecutionResult::Done();
}

void TConflictDetector::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(ConflictDetector, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TConflictDetector::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    AFL_VERIFY(source.IsConflicting());
    // the method returns true for conflicting portions, even if they are aborted already
    AFL_VERIFY(source.AddTxConflict());
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TDeletionFilter::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(DeletionFilter, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TDeletionFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    if (!source.GetStageData().GetTable().HasColumn((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
        ReportTracing(source, step);
        return TExecutionResult::Done();
    }
    auto filterTable = source.GetStageData().GetTable().ToTable(std::set<ui32>({ (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG }));
    if (!filterTable) {
        ReportTracing(source, step);
        return TExecutionResult::Done();
    }
    AFL_VERIFY(filterTable->column(0)->type()->id() == arrow::boolean()->id());
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    for (auto&& i : filterTable->column(0)->chunks()) {
        auto filterFlags = static_pointer_cast<arrow::BooleanArray>(i);
        for (ui32 i = 0; i < filterFlags->length(); ++i) {
            filter.Add(!filterFlags->GetView(i));
        }
    }
    source.MutableStageData().AddFilter(filter);
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TShardingFilter::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(ShardingFilter, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TShardingFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source.GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    const std::set<ui32> ids = source.GetContext()->GetCommonContext()->GetResolver()->GetColumnIdsSetVerified(shardingInfo->GetColumnNames());
    auto filter = shardingInfo->GetFilter(source.GetStageData().GetTable().ToTable(ids, source.GetContext()->GetCommonContext()->GetResolver()));
    source.MutableStageData().AddFilter(filter);
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TFilterCutLimit::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(FilterCutLimit, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TFilterCutLimit::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    source.MutableStageData().CutFilter(source.GetRecordsCount(), Limit, Reverse);
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TDetectInMemFlag::ReportTracing(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step, const ui64 columnRawBytes, const ui64 columnBlobBytes) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(DetectInMemFlag, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, columnBlobBytes, columnRawBytes, source.IsSourceInMemory(),
        source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TDetectInMemFlag::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    if (!source.NeedPortionData()) {
        source.SetSourceInMemory(true);
        source.MutableAs<IDataSource>()->InitUsedRawBytes();
    }
    if (source.HasSourceInMemoryFlag()) {
        ReportTracing(source, step, 0UL, 0UL);
        return TExecutionResult::Done();
    }
    ui64 columnRawBytes = 0;
    ui64 columnBlobBytes = 0;
    if (Columns.GetColumnsCount() && source.GetContext()->GetReadMetadata()->GetProgram().GetGraphOptional() &&
        !source.GetContext()->GetReadMetadata()->GetProgram().GetChainVerified()->HasAggregations()) {
        columnRawBytes = source.GetColumnRawBytes(Columns.GetColumnIds());
        columnBlobBytes = source.GetColumnBlobBytes(Columns.GetColumnIds());
        source.SetSourceInMemory(columnRawBytes < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source.SetSourceInMemory(true);
    }
    ReportTracing(source, step, columnRawBytes, columnBlobBytes);
    return TExecutionResult::Done();
}

namespace {
class TApplySourceResult: public IApplyAction {
private:
    using TBase = IDataTasksProcessor::ITask;
    std::unique_ptr<NCommon::TDataSourceLease> SourceLease;
    TFetchingScriptCursor Step;

public:
    class TStartJob: public NCommon::IAsyncJob {
    private:
        const TFetchingScriptCursor Step;
        const ui32 RecordsCount;

    public:
        TStartJob(const TFetchingScriptCursor& step, const ui32 recordsCount)
            : Step(step)
            , RecordsCount(recordsCount)
        {
        }

        virtual void Start(std::unique_ptr<NCommon::TDataSourceLease> sourceLease) override {
            auto& source = sourceLease->GetSource();
            const auto& commonContext = *source.GetContext()->GetCommonContext();
            const auto scanActorId = commonContext.GetScanActorId();
            auto guard = commonContext.GetCounters().GetResultsForSourceGuard();
            const ui64 sourceId = source.GetSourceId();
            const ui64 blobBytes = source.GetTotalBytesRead();
            const ui64 rawBytes = source.GetAs<IDataSource>()->GetUsedRawBytes();
            const ui32 totalRows = source.GetRecordsCount();
            const ui64 reservedMemory = source.GetReservedMemory();
            auto applyAction = std::make_shared<TApplySourceResult>(std::move(sourceLease), Step);
            NActors::TActivationContext::AsActorContext().Send(
                scanActorId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(std::move(applyAction), std::move(guard), sourceId, blobBytes,
                                 rawBytes, RecordsCount, totalRows, reservedMemory));
        }
    };

    TApplySourceResult(std::unique_ptr<NCommon::TDataSourceLease> sourceLease, const TFetchingScriptCursor& step)
        : SourceLease(std::move(sourceLease))
        , Step(step)
    {
    }

    virtual ui64 GetSourceId() const override {
        return SourceLease ? SourceLease->GetSource().GetSourceId() : 0;
    }

    virtual bool DoApply(IDataReader& indexedDataRead) override {
        auto* plainReader = static_cast<TPlainReadData*>(&indexedDataRead);
        auto& source = SourceLease->GetSource();
        source.MutableAs<IDataSource>()->SetCursor(std::move(Step));
        source.StartSyncSection();
        const ui32 syncPointIndex = source.GetAs<IDataSource>()->GetPurposeSyncPointIndex();
        plainReader->MutableScanner().GetSyncPoint(syncPointIndex)->OnSourcePrepared(std::move(SourceLease), *plainReader);
        return true;
    }
};

}   // namespace

void TUpdateAggregatedMemoryStep::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(UpdateAggregatedMemory, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(),
        source.GetSourceId(), step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TUpdateAggregatedMemoryStep::DoExecuteInplace(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    if (auto* portionSource = source.MutableOptionalAs<TPortionDataSource>()) {
        portionSource->ActualizeAggregatedMemoryGuards();
    }
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TInitializeSourceStep::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(InitializeSource, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TInitializeSourceStep::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    source.MutableAs<IDataSource>()->InitializeProcessing();
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TPortionAccessorFetchedStep::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(PortionAccessorFetched, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(),
        source.GetSourceId(), step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TPortionAccessorFetchedStep::DoExecuteInplace(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    source.MutableAs<IDataSource>()->InitUsedRawBytes();
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TStepAggregationSources::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(AggregationSources, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TStepAggregationSources::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    AFL_VERIFY(source.GetType() == IDataSource::EType::SimpleAggregation);
    const auto& aggrSource = static_cast<const TAggregationDataSource&>(source);
    std::vector<std::unique_ptr<NArrow::NSSA::TAccessorsCollection>> collections;
    for (auto&& i : aggrSource.GetSources()) {
        collections.emplace_back(i->GetSource().MutableStageData().ExtractTable());
    }
    auto conclusion = Aggregator->Execute(std::move(collections), source.MutableStageData().MutableTable());
    if (conclusion.IsFail()) {
        return conclusion;
    }
    source.BuildStageResult();
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

void TCleanAggregationSources::ReportTracing(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(CleanAggregationSources, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(),
        source.GetSourceId(), step.GetStepIndex(), step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TCleanAggregationSources::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    AFL_VERIFY(source.GetType() == IDataSource::EType::SimpleAggregation);
    const auto& aggrSource = static_cast<const TAggregationDataSource&>(source);
    for (auto&& i : aggrSource.GetSources()) {
        i->GetSource().MutableAs<IDataSource>()->ClearResult();
    }
    ReportTracing(source, step);
    return TExecutionResult::Done();
}

bool TBuildResultStep::IsPageSkippedByFilter(NCommon::IDataSource& source) const {
    const auto& notAppliedFilter = source.GetStageResult().GetNotAppliedFilter();
    if (notAppliedFilter && !notAppliedFilter->IsTotalAllowFilter()) {
        const auto pageFilter = notAppliedFilter->Slice(StartIndex, RecordsCount);
        return pageFilter.IsTotalDenyFilter();
    }
    return false;
}

void TBuildResultStep::ReportTracing(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step, const TDuration executionDurationMs) const {
    if (!LWPROBE_ENABLED(BuildResult) && !NLWTrace::HasShuttles(source.GetDataSourceOrbit())) {
        return;
    }
    const TDuration durationMs = source.GetAndResetWaitDuration();
    ui32 pageFilteredRowsCount = RecordsCount;
    const auto& notAppliedFilter = source.GetStageResult().GetNotAppliedFilter();
    if (notAppliedFilter && !notAppliedFilter->IsTotalAllowFilter()) {
        const auto pageFilter = notAppliedFilter->Slice(StartIndex, RecordsCount);
        pageFilteredRowsCount = pageFilter.GetFilteredCount().value_or(RecordsCount);
    }
    LWTRACK(BuildResult, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, executionDurationMs, pageFilteredRowsCount, RecordsCount,
        source.GetReservedMemory(), source.GetSourcesAheadQueueWaitDuration(), source.GetSourcesAhead());
}

std::shared_ptr<arrow::Table> TBuildResultStep::BuildPageResultBatch(NCommon::IDataSource& source) const {
    if (IsPageSkippedByFilter(source)) {
        return nullptr;
    }
    auto context = source.GetContext();
    NArrow::TGeneralContainer::TTableConstructionContext contextTableConstruct;
    if (!source.IsSourceInMemory()) {
        contextTableConstruct.SetStartIndex(StartIndex).SetRecordsCount(RecordsCount);
    } else {
        AFL_VERIFY(StartIndex == 0);
        AFL_VERIFY(RecordsCount == source.GetStageResult().GetBatch()->num_rows())("records_count", RecordsCount)(
                                     "batch", source.GetStageResult().GetBatch()->num_rows());
    }
    contextTableConstruct.SetFilter(source.GetStageResult().GetNotAppliedFilter());
    if (source.GetStageResult().IsEmpty()) {
        return nullptr;
    }
    auto resultBatch = source.GetStageResult().GetBatch()->BuildTableVerified(contextTableConstruct);
    return resultBatch->num_rows() ? resultBatch : nullptr;
}

TConclusion<TExecutionResult> TBuildResultStep::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TMonotonic startExecution = TMonotonic::Now();
    auto context = source.GetContext();
    auto resultBatch = BuildPageResultBatch(source);
    auto* sSource = source.MutableAs<IDataSource>();
    const ui32 recordsCount = resultBatch ? resultBatch->num_rows() : 0;
    YDB_LOG_DEBUG("",
        {"event", "TBuildResultStep"},
        {"sourceIdx", source.GetSourceIdx()},
        {"count", recordsCount});
    context->GetCommonContext()->GetCounters().OnSourceFinished(source.GetRecordsCount(), sSource->GetUsedRawBytes(), recordsCount);
    sSource->MutableResultRecordsCount() += recordsCount;
    if (!resultBatch || !resultBatch->num_rows()) {
        YDB_LOG_DEBUG("",
            {"emptySource", sSource->DebugJson().GetStringRobust()});
    }
    source.MutableStageResult().SetResultChunk(std::move(resultBatch), StartIndex, RecordsCount);
    ReportTracing(source, step, TMonotonic::Now() - startExecution);
    return TExecutionResult::Pending(std::make_shared<TApplySourceResult::TStartJob>(step, recordsCount));
}

void TPrepareResultStep::ReportTracing(
    NCommon::IDataSource& source, const TFetchingScriptCursor& step, const TDuration executionDurationMs) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(PrepareResult, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), durationMs, executionDurationMs, source.GetFilteredRowsCount(), source.GetReservedMemory(),
        source.GetSourcesAheadQueueWaitDuration(), source.GetSourcesAhead());
}

TConclusion<TExecutionResult> TPrepareResultStep::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const TMonotonic startExecution = TMonotonic::Now();
    const auto context = source.GetContext();
    NCommon::TFetchingScriptBuilder acc(*context);
    if (source.IsSourceInMemory()) {
        AFL_VERIFY(source.GetStageResult().GetPagesToResultVerified().size() == 1);
    }
    AFL_VERIFY(!source.GetStageResult().IsEmpty());
    auto* sSource = source.MutableAs<IDataSource>();
    if (sSource->GetIsStartedByCursor()) {
        const auto& scanCursor = context->GetCommonContext()->GetScanCursor();
        while (!source.GetStageResult().IsFinished()) {
            const auto& page = source.GetStageResult().GetPagesToResultVerified().front();
            if (scanCursor->CheckSourceIntervalUsage(source.GetSourceIdx(), page.GetIndexStart(), page.GetRecordsCount())) {
                break;
            }
            YDB_LOG_WARN("",
                {"event", "TPrepareResultStep_ResultStep_SKIP_CURSOR"},
                {"sourceIdx", source.GetSourceIdx()});
            source.MutableStageResult().ExtractPageForResult();
        }
    }
    for (const auto& page : source.GetStageResult().GetPagesToResultVerified()) {
        YDB_LOG_DEBUG("",
            {"event", "TPrepareResultStep_ResultStep"},
            {"sourceIdx", source.GetSourceIdx()});
        acc.AddStep(std::make_shared<TBuildResultStep>(page.GetIndexStart(), page.GetRecordsCount()));
    }
    auto plan = std::move(acc).Build();
    ReportTracing(source, step, TMonotonic::Now() - startExecution);
    if (plan->IsFinished(0)) {
        YDB_LOG_DEBUG("",
            {"event", "TPrepareResultStep_AllPagesSkippedByCursor"},
            {"sourceIdx", source.GetSourceIdx()});
        AFL_VERIFY(source.GetStageResult().IsFinished());
        source.MutableStageResult().SetEmptyResultChunk();
        context->GetCommonContext()->GetCounters().OnSourceFinished(source.GetRecordsCount(), sSource->GetUsedRawBytes(), 0);
        return TExecutionResult::Pending(std::make_shared<TApplySourceResult::TStartJob>(step, 0));
    }
    source.MutableAs<IDataSource>()->InitFetchingPlan(plan);
    if (StartResultBuildingInplace) {
        TFetchingScriptCursor cursor(plan, 0);
        return cursor.Execute(source);
    } else {
        return TExecutionResult::Done();
    }
}

void TDuplicateFilter::TFilterSubscriber::ReportTracing(NCommon::IDataSource& source) const {
    const TDuration durationMs = source.GetAndResetWaitDuration();
    LWTRACK(Deduplication, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        Step.GetStepIndex(), Step.GetTracingName(), durationMs, source.GetRecordsCount(), source.GetReservedMemory());
}

void TDuplicateFilter::TFilterSubscriber::OnFilterReady(NArrow::TColumnFilter&& filter) {
    auto& source = SourceLease->GetSource();
    YDB_LOG_TRACE("",
        {"event", "fetch_filter"},
        {"source", source.GetSourceIdx()},
        {"filter", filter.DebugString()},
        {"aborted", source.GetContext()->IsAborted()});
    if (source.GetContext()->IsAborted()) {
        return;
    }
    AFL_VERIFY(filter.GetRecordsCountVerified() == source.GetRecordsCount())("filter", filter.GetRecordsCountVerified())(
                                                     "source", source.GetRecordsCount());

    ReportTracing(source);

    if (const std::shared_ptr<NArrow::TColumnFilter> appliedFilter = source.GetStageData().GetAppliedFilter()) {
        filter = filter.ApplyFilterFrom(*appliedFilter);
    }
    source.MutableStageData().AddFilter(std::move(filter));
    Step.Next();
    const auto& commonContext = *source.GetContext()->GetCommonContext();
    const auto scanActorId = commonContext.GetScanActorId();
    auto task = std::make_shared<TStepAction>(std::move(SourceLease), std::move(Step), scanActorId, false);
    commonContext.SendTaskToExecute(task);
}

void TDuplicateFilter::TFilterSubscriber::OnFailure(const TString& reason) {
    SourceLease->GetSource().GetContext()->GetCommonContext()->AbortWithError("cannot build duplicate filter: " + reason);
}

TDuplicateFilter::TFilterSubscriber::TFilterSubscriber(std::unique_ptr<NCommon::TDataSourceLease> sourceLease, const TFetchingScriptCursor& step)
    : SourceLease(std::move(sourceLease))
    , Step(step)
    , TaskGuard(SourceLease->GetSource().GetContext()->GetCommonContext()->GetCounters().GetFilterFetchingGuard())
{
}

void TDuplicateFilter::TFilterSubscriber::TStartJob::Start(std::unique_ptr<NCommon::TDataSourceLease> sourceLease) {
    const auto& portionSource = *sourceLease->GetSource().GetAs<TPortionDataSource>();
    auto subscriber = std::make_shared<TFilterSubscriber>(std::move(sourceLease), Step);
    NActors::TActivationContext::AsActorContext().Send(DuplicatesManager, new NDuplicateFiltering::TEvRequestFilter(portionSource, subscriber));
}

TConclusion<TExecutionResult> TDuplicateFilter::DoExecuteInplace(NCommon::IDataSource& source, const TFetchingScriptCursor& step) const {
    const auto context = std::static_pointer_cast<TSpecialReadContext>(source.GetContext());
    const auto duplicatesManager = context->GetDuplicatesManager();
    if (!duplicatesManager) {
        AFL_VERIFY(!context->IsActive());
        return TConclusionStatus::Fail("duplicates manager is unregistered by scan abort");
    }
    return TExecutionResult::Pending(std::make_shared<TFilterSubscriber::TStartJob>(duplicatesManager, step));
}

}   // namespace NKikimr::NOlap::NReader::NTrivial
