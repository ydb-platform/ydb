#include "fetching.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

TConclusion<bool> IFetchingStep::DoExecuteInplace(
    const std::shared_ptr<NCommon::IDataSource>& sourceExt, const TFetchingScriptCursor& step) const {
    const auto source = std::static_pointer_cast<IDataSource>(sourceExt);
    return DoExecuteInplace(source, step);
}

ui64 IFetchingStep::GetProcessingDataSize(const std::shared_ptr<NCommon::IDataSource>& source) const {
    return GetProcessingDataSize(std::static_pointer_cast<IDataSource>(source));
}

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(
        source->GetStageData().GetTable()->ToTable(source->GetContext()->GetReadMetadata()->GetPKRangesFilter().GetColumnIds(
                                                       source->GetContext()->GetReadMetadata()->GetResultSchema()->GetIndexInfo()),
            source->GetContext()->GetCommonContext()->GetResolver(), true));
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
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

TConclusion<bool> TDeletionFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
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

TConclusion<bool> TShardingFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    NYDBTest::TControllers::GetColumnShardController()->OnSelectShardingFilter();
    const auto& shardingInfo = source->GetContext()->GetReadMetadata()->GetRequestShardingInfo()->GetShardingInfo();
    const std::set<ui32> ids = source->GetContext()->GetCommonContext()->GetResolver()->GetColumnIdsSetVerified(shardingInfo->GetColumnNames());
    auto filter =
        shardingInfo->GetFilter(source->GetStageData().GetTable()->ToTable(ids, source->GetContext()->GetCommonContext()->GetResolver()));
    source->MutableStageData().AddFilter(filter);
    return true;
}

NKikimr::TConclusion<bool> TFilterCutLimit::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableStageData().CutFilter(source->GetRecordsCount(), Limit, Reverse);
    return true;
}

TConclusion<bool> TPortionAccessorFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("sacc"));
    return !source->StartFetchingAccessor(source, step);
}

TConclusion<bool> TDetectInMem::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    const auto& chainProgram = source->GetContext()->GetReadMetadata()->GetProgram().GetChainVerified();
    if (Columns.GetColumnsCount() && !chainProgram->HasAggregations()) {
        source->SetSourceInMemory(
            source->GetColumnRawBytes(Columns.GetColumnIds()) < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source->SetSourceInMemory(true);
    }
    AFL_VERIFY(source->GetStageData().HasPortionAccessor());
    auto plan = source->GetContext()->GetColumnsFetchingPlan(source);
    source->InitFetchingPlan(plan);
    TFetchingScriptCursor cursor(plan, 0);
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("sdmem"));
    auto task = std::make_shared<TStepAction>(source, std::move(cursor), source->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    return false;
}

namespace {
class TApplySourceResult: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
    YDB_READONLY_DEF(std::shared_ptr<arrow::Table>, Result);
    YDB_READONLY_DEF(std::shared_ptr<IDataSource>, Source);
    YDB_READONLY(ui32, StartIndex, 0);
    YDB_READONLY(ui32, OriginalRecordsCount, 0);
    NColumnShard::TCounterGuard Guard;
    TFetchingScriptCursor Step;

public:
    TString GetTaskClassIdentifier() const override {
        return "TApplySourceResult";
    }

    TApplySourceResult(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& result, const ui32 startIndex,
        const ui32 originalRecordsCount, const TFetchingScriptCursor& step)
        : TBase(NActors::TActorId())
        , Result(result)
        , Source(source)
        , StartIndex(startIndex)
        , OriginalRecordsCount(originalRecordsCount)
        , Guard(source->GetContext()->GetCommonContext()->GetCounters().GetResultsForSourceGuard())
        , Step(step) {
    }

    virtual TConclusionStatus DoExecuteImpl() override {
        AFL_VERIFY(false)("event", "not applicable");
        return TConclusionStatus::Success();
    }
    virtual bool DoApply(IDataReader& indexedDataRead) const override {
        auto* plainReader = static_cast<TPlainReadData*>(&indexedDataRead);
        auto resultCopy = Result;
        Source->SetCursor(Step);
        plainReader->MutableScanner().OnSourceReady(Source, std::move(resultCopy), StartIndex, OriginalRecordsCount, *plainReader);
        return true;
    }
};

}   // namespace

TConclusion<bool> TBuildResultStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    auto context = source->GetContext();
    NArrow::TGeneralContainer::TTableConstructionContext contextTableConstruct;
    if (!source->IsSourceInMemory()) {
        contextTableConstruct.SetStartIndex(StartIndex).SetRecordsCount(RecordsCount);
    } else {
        AFL_VERIFY(StartIndex == 0);
        AFL_VERIFY(RecordsCount == source->GetRecordsCount())("records_count", RecordsCount)("source", source->GetRecordsCount());
    }
    std::shared_ptr<arrow::Table> resultBatch;
    if (!source->GetStageResult().IsEmpty()) {
        resultBatch = source->GetStageResult().GetBatch()->BuildTableVerified(contextTableConstruct);
        if (auto filter = source->GetStageResult().GetNotAppliedFilter()) {
            AFL_VERIFY(filter->Apply(resultBatch, NArrow::TColumnFilter::TApplyContext(StartIndex, RecordsCount).SetTrySlices(true)));
        }
    }

    NActors::TActivationContext::AsActorContext().Send(context->GetCommonContext()->GetScanActorId(),
        new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
            std::make_shared<TApplySourceResult>(source, std::move(resultBatch), StartIndex, RecordsCount, step)));
    return false;
}

TConclusion<bool> TPrepareResultStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    NCommon::TFetchingScriptBuilder acc(*source->GetContext());
    if (source->IsSourceInMemory()) {
        AFL_VERIFY(source->GetStageResult().GetPagesToResultVerified().size() == 1);
    }
    for (auto&& i : source->GetStageResult().GetPagesToResultVerified()) {
        if (source->GetIsStartedByCursor() && !source->GetContext()->GetCommonContext()->GetScanCursor()->CheckSourceIntervalUsage(
                                                  source->GetSourceId(), i.GetIndexStart(), i.GetRecordsCount())) {
            continue;
        }
        acc.AddStep(std::make_shared<TBuildResultStep>(i.GetIndexStart(), i.GetRecordsCount()));
    }
    auto plan = std::move(acc).Build();
    AFL_VERIFY(!plan->IsFinished(0));
    source->InitFetchingPlan(plan);

    TFetchingScriptCursor cursor(plan, 0);
    auto task = std::make_shared<TStepAction>(source, std::move(cursor), source->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    return false;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
