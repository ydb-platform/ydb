#include "fetching.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source->GetStageData().GetTable()->ToGeneralContainer(source->GetContext()->GetCommonContext()->GetResolver(),
            source->GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                source->GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()),
            true));
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter =
        MakeSnapshotFilter(source->GetStageData().GetTable()->ToTable(
                               std::set<ui32>({ (ui32)IIndexInfo::ESpecialColumn::PLAN_STEP, (ui32)IIndexInfo::ESpecialColumn::TX_ID }),
                               source->GetContext()->GetCommonContext()->GetResolver()),
            source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source->GetRecordsCount()) != source->GetRecordsCount()) {
        if (source->AddTxConflict()) {
            return true;
        }
    }
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TDeletionFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    if (!source->GetStageData().GetTable()->HasColumn((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
        return true;
    }
    auto filterTable = source->GetStageData().GetTable()->ToTable(std::set<ui32>({ (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG }));
    if (!filterTable) {
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
    return true;
}

TConclusion<bool> TShardingFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source->GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    const std::set<ui32> ids = source->GetContext()->GetCommonContext()->GetResolver()->GetColumnIdsSetVerified(shardingInfo->GetColumnNames());
    auto filter =
        shardingInfo->GetFilter(source->GetStageData().GetTable()->ToTable(ids, source->GetContext()->GetCommonContext()->GetResolver()));
    source->MutableStageData().AddFilter(filter);
    return true;
}

NKikimr::TConclusion<bool> TFilterCutLimit::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableStageData().CutFilter(source->GetRecordsCount(), Limit, Reverse);
    return true;
}

TConclusion<bool> TStartPortionAccessorFetchingStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("sacc"));
    if (source->HasPortionAccessor()) {
        return true;
    }
    return !source->MutableAs<IDataSource>()->StartFetchingAccessor(source, step);
}

TConclusion<bool> TDetectScript::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto plan = source->GetContext()->GetColumnsFetchingPlan(source);
    source->MutableAs<IDataSource>()->InitFetchingPlan(plan);
    TFetchingScriptCursor cursor(plan, 0);
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("sdmem"));
    return cursor.Execute(source);
}

TConclusion<bool> TDetectInMemFlag::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    if (source->HasSourceInMemoryFlag()) {
        return true;
    }
    const auto& chainProgram = source->GetContext()->GetReadMetadata()->GetProgram().GetChainVerified();
    if (Columns.GetColumnsCount() && !chainProgram->HasAggregations()) {
        source->SetSourceInMemory(
            source->GetColumnRawBytes(Columns.GetColumnIds()) < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source->SetSourceInMemory(true);
    }
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

    virtual bool DoApply(IDataReader& indexedDataRead) override {
        auto* plainReader = static_cast<TPlainReadData*>(&indexedDataRead);
        Source->MutableAs<IDataSource>()->SetCursor(std::move(Step));
        Source->StartSyncSection();
        plainReader->MutableScanner().GetResultSyncPoint()->OnSourcePrepared(std::move(Source), *plainReader);
        return true;
    }
};


}   // namespace

NKikimr::TConclusion<bool> TInitializeSourceStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableAs<IDataSource>()->InitializeProcessing(source);
    return true;
}

TConclusion<bool> TPortionAccessorFetchedStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableAs<IDataSource>()->InitUsedRawBytes();
    return true;
}

TConclusion<bool> TStepAggregationSources::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source->GetAs<IDataSource>()->GetType() == IDataSource::EType::Aggregation);
    auto* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
    std::vector<std::shared_ptr<NArrow::NSSA::TAccessorsCollection>> collections;
    for (auto&& i : aggrSource->GetSources()) {
        collections.emplace_back(i->GetStageData().GetTable());
    }
    auto conclusion = Aggregator->Execute(collections, source->GetStageData().GetTable());
    if (conclusion.IsFail()) {
        return conclusion;
    }
    source->BuildStageResult(source);
    return true;
}

TConclusion<bool> TCleanAggregationSources::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source->GetAs<IDataSource>()->GetType() == IDataSource::EType::Aggregation);
    auto* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
    for (auto&& i : aggrSource->GetSources()) {
        i->MutableAs<IDataSource>()->ClearResult();
    }
    return true;
}

TConclusion<bool> TBuildResultStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    auto context = source->GetContext();
    NArrow::TGeneralContainer::TTableConstructionContext contextTableConstruct;
    if (!source->IsSourceInMemory()) {
        contextTableConstruct.SetStartIndex(StartIndex).SetRecordsCount(RecordsCount);
    } else {
        AFL_VERIFY(StartIndex == 0);
        AFL_VERIFY(RecordsCount == source->GetRecordsCount())("records_count", RecordsCount)("source", source->GetRecordsCount());
    }
    contextTableConstruct.SetFilter(source->GetStageResult().GetNotAppliedFilter());
    std::shared_ptr<arrow::Table> resultBatch;
    if (!source->GetStageResult().IsEmpty()) {
        resultBatch = source->GetStageResult().GetBatch()->BuildTableVerified(contextTableConstruct);
        if (!resultBatch->num_rows()) {
            resultBatch = nullptr;
        }
    }
    auto* sSource = source->MutableAs<IDataSource>();
    const ui32 recordsCount = resultBatch ? resultBatch->num_rows() : 0;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TBuildResultStep")("source_id", source->GetSourceId())("count", recordsCount);
    context->GetCommonContext()->GetCounters().OnSourceFinished(source->GetRecordsCount(), sSource->GetUsedRawBytes(), recordsCount);
    sSource->MutableResultRecordsCount() += recordsCount;
    if (!resultBatch || !resultBatch->num_rows()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("empty_source", sSource->DebugJson().GetStringRobust());
    }
    source->MutableStageResult().SetResultChunk(std::move(resultBatch), StartIndex, RecordsCount);
    NActors::TActivationContext::AsActorContext().Send(context->GetCommonContext()->GetScanActorId(),
        new NColumnShard::TEvPrivate::TEvTaskProcessedResult(std::make_shared<TApplySourceResult>(source, step),
            source->GetContext()->GetCommonContext()->GetCounters().GetResultsForSourceGuard()));
    return false;
}

TConclusion<bool> TPrepareResultStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    const auto context = source->GetContext();
    NCommon::TFetchingScriptBuilder acc(*context);
    if (source->IsSourceInMemory()) {
        AFL_VERIFY(source->GetStageResult().GetPagesToResultVerified().size() == 1);
    }
    AFL_VERIFY(!source->GetStageResult().IsEmpty());
    auto* sSource = source->MutableAs<IDataSource>();
    for (auto&& i : source->GetStageResult().GetPagesToResultVerified()) {
        if (sSource->GetIsStartedByCursor() && !context->GetCommonContext()->GetScanCursor()->CheckSourceIntervalUsage(
                                                  source->GetSourceId(), i.GetIndexStart(), i.GetRecordsCount())) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TPrepareResultStep_ResultStep_SKIP_CURSOR")("source_id", source->GetSourceId());
            continue;
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TPrepareResultStep_ResultStep")("source_id", source->GetSourceId());
        }
        acc.AddStep(std::make_shared<TBuildResultStep>(i.GetIndexStart(), i.GetRecordsCount()));
    }
    auto plan = std::move(acc).Build();
    AFL_VERIFY(!plan->IsFinished(0));
    source->MutableAs<IDataSource>()->InitFetchingPlan(plan);
    if (source->GetAs<IDataSource>()->NeedFullAnswer()) {
        TFetchingScriptCursor cursor(plan, 0);
        const auto& commonContext = *context->GetCommonContext();
        auto sCopy = source;
        auto task = std::make_shared<TStepAction>(std::move(sCopy), std::move(cursor), commonContext.GetScanActorId(), false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, commonContext.GetConveyorProcessId());
        return false;
    } else {
        return true;
    }
}

void TDuplicateFilter::TFilterSubscriber::OnFilterReady(NArrow::TColumnFilter&& filter) {
    if (auto source = Source.lock()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetch_filter")("source", source->GetSourceId())(
            "filter", filter.DebugString())("aborted", source->GetContext()->IsAborted());
        if (source->GetContext()->IsAborted()) {
            OnDone();
            return;
        }
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
    OnDone();
}

void TDuplicateFilter::TFilterSubscriber::OnFailure(const TString& reason) {
    if (auto source = Source.lock()) {
        source->GetContext()->GetCommonContext()->AbortWithError("cannot build duplicate filter: " + reason);
    }
    OnDone();
}

TDuplicateFilter::TFilterSubscriber::TFilterSubscriber(const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step)
    : Source(source)
    , Step(step)
    , TaskGuard(source->GetContext()->GetCommonContext()->GetCounters().GetFilterFetchingGuard()) {
}

TConclusion<bool> TDuplicateFilter::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& source, const TFetchingScriptCursor& step) const {
    source->MutableAs<IDataSource>()->StartFetchingDuplicateFilter(std::make_shared<TFilterSubscriber>(source, step));
    return false;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
