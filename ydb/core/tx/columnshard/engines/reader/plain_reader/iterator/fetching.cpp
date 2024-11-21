#include "fetching.h"
#include "source.h"

#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/tx/columnshard/engines/filter.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NPlain {

bool TStepAction::DoApply(IDataReader& /*owner*/) const {
    if (FinishedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
        Source->SetIsReady();
    }
    return true;
}

TConclusionStatus TStepAction::DoExecuteImpl() {
    if (Source->GetContext()->IsAborted()) {
        return TConclusionStatus::Success();
    }
    auto executeResult = Cursor.Execute(Source);
    if (!executeResult) {
        return executeResult;
    }
    if (*executeResult) {
        Source->Finalize();
        FinishedFlag = true;
    }
    return TConclusionStatus::Success();
}

TConclusion<bool> TColumnBlobsFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingColumns(source, step, Columns);
}

ui64 TColumnBlobsFetchingStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnBlobBytes(Columns.GetColumnIds());
}

TConclusion<bool> TIndexBlobsFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingIndexes(source, step, Indexes);
}

TConclusion<bool> TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

ui64 TAssemblerStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnRawBytes(Columns->GetColumnIds());
}

TConclusion<bool> TOptionalAssemblerStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->AssembleColumns(Columns, !source->GetExclusiveIntervalOnly() || !source->IsSourceInMemory());
    return true;
}

ui64 TOptionalAssemblerStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnsVolume(Columns->GetColumnIds(), EMemType::RawSequential);
}

TConclusion<bool> TFilterProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source);
    AFL_VERIFY(Step);
    auto filter = Step->BuildFilter(source->GetStageData().GetTable());
    if (!filter.ok()) {
        return TConclusionStatus::Fail(filter.status().message());
    }
    source->MutableStageData().AddFilter(*filter);
    return true;
}

TConclusion<bool> TPredicateFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter =
        source->GetContext()->GetReadMetadata()->GetPKRangesFilter().BuildFilter(source->GetStageData().GetTable()->BuildTableVerified());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TSnapshotFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filter = MakeSnapshotFilter(
        source->GetStageData().GetTable()->BuildTableVerified(), source->GetContext()->GetReadMetadata()->GetRequestSnapshot());
    if (filter.GetFilteredCount().value_or(source->GetRecordsCount()) != source->GetRecordsCount()) {
        if (source->AddTxConflict()) {
            return true;
        }
    }
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TDeletionFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    auto filterTable = source->GetStageData().GetTable()->BuildTableOptional(std::set<std::string>({ TIndexInfo::SPEC_COL_DELETE_FLAG }));
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
    auto filter = shardingInfo->GetFilter(source->GetStageData().GetTable()->BuildTableVerified());
    source->MutableStageData().AddFilter(filter);
    return true;
}

TConclusion<bool> TBuildFakeSpec::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& f : IIndexInfo::ArrowSchemaSnapshot()->fields()) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::GetConst(f->type(), NArrow::DefaultScalar(f->type()), Count));
    }
    source->MutableStageData().AddBatch(
        std::make_shared<NArrow::TGeneralContainer>(arrow::RecordBatch::Make(TIndexInfo::ArrowSchemaSnapshot(), Count, columns)));
    return true;
}

TConclusion<bool> TApplyIndexStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->ApplyIndex(IndexChecker);
    return true;
}

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    NMiniKQL::TThrowingBindTerminator bind;
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->GetStageData().IsEmpty()) {
            source->OnEmptyStageData();
            break;
        }
        auto step = Script->GetStep(CurrentStepIdx);
        TMemoryProfileGuard mGuard("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName(),
            IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx);
        AFL_VERIFY(!CurrentStartInstant);
        CurrentStartInstant = TMonotonic::Now();
        AFL_VERIFY(!CurrentStartDataSize);
        CurrentStartDataSize = step->GetProcessingDataSize(source);
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        if (!resultStep) {
            return resultStep;
        }
        if (!*resultStep) {
            return false;
        }
        FlushDuration();
        ++CurrentStepIdx;
    }
    return true;
}

bool TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
    const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) {
    auto data = Source.lock();
    if (!data || data->GetContext()->IsAborted()) {
        guard->Release();
        return false;
    }
    data->RegisterAllocationGuard(std::move(guard));
    Step.Next();
    auto task = std::make_shared<TStepAction>(data, std::move(Step), data->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    return true;
}

TAllocateMemoryStep::TFetchingStepAllocation::TFetchingStepAllocation(
    const std::shared_ptr<IDataSource>& source, const ui64 mem, const TFetchingScriptCursor& step)
    : TBase(mem)
    , Source(source)
    , Step(step)
    , TasksGuard(source->GetContext()->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard()) {
}

void TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocationImpossible(const TString& errorMessage) {
    auto sourcePtr = Source.lock();
    if (sourcePtr) {
        sourcePtr->GetContext()->GetCommonContext()->AbortWithError(
            "cannot allocate memory for step " + Step.GetName() + ": '" + errorMessage + "'");
    }
}

TConclusion<bool> TAllocateMemoryStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {

    ui64 size = PredefinedSize.value_or(0);
    for (auto&& i : Packs) {
        ui32 sizeLocal = source->GetColumnsVolume(i.GetColumns().GetColumnIds(), i.GetMemType());
        if (source->GetStageData().GetUseFilter() && source->GetContext()->GetReadMetadata()->Limit && i.GetMemType() != EMemType::Blob) {
            const ui32 filtered =
                source->GetStageData().GetFilteredCount(source->GetRecordsCount(), source->GetContext()->GetReadMetadata()->Limit);
            if (filtered < source->GetRecordsCount()) {
                sizeLocal = sizeLocal * 1.0 * filtered / source->GetRecordsCount();
            }
        }
        size += sizeLocal;
    }


    auto allocation = std::make_shared<TFetchingStepAllocation>(source, size, step);
    NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(source->GetContext()->GetProcessMemoryControlId(),
        source->GetContext()->GetCommonContext()->GetScanId(), source->GetFirstIntervalId(), { allocation }, (ui32)StageIndex);
    return false;
}

ui64 TAllocateMemoryStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const {
    return 0;
}

TString TFetchingScript::DebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    for (auto&& i : Steps) {
        if (i->GetSumDuration() > TDuration::MilliSeconds(10)) {
            sbBranch << "{" << i->DebugString() << "};";
        }
    }
    if (!sbBranch) {
        return "";
    }
    sb << "{branch:" << BranchName << ";limit:" << Limit << ";";
    if (FinishInstant && StartInstant) {
        sb << "duration:" << *FinishInstant - *StartInstant << ";";
    }

    sb << "steps_10Ms:[" << sbBranch << "]}";
    return sb;
}

TFetchingScript::TFetchingScript(const TSpecialReadContext& context)
    : Limit(context.GetReadMetadata()->Limit) {
}

void TFetchingScript::Allocation(const std::set<ui32>& entityIds, const EStageFeaturesIndexes stage, const EMemType mType) {
    if (Steps.size() == 0) {
        AddStep<TAllocateMemoryStep>(entityIds, mType, stage);
    } else {
        std::optional<ui32> addIndex;
        for (i32 i = Steps.size() - 1; i >= 0; --i) {
            if (auto allocation = std::dynamic_pointer_cast<TAllocateMemoryStep>(Steps[i])) {
                if (allocation->GetStage() == stage) {
                    allocation->AddAllocation(entityIds, mType);
                    return;
                } else {
                    addIndex = i + 1;
                }
                break;
            } else if (std::dynamic_pointer_cast<TAssemblerStep>(Steps[i])) {
                continue;
            } else if (std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps[i])) {
                continue;
            } else {
                addIndex = i + 1;
                break;
            }
        }
        AFL_VERIFY(addIndex);
        InsertStep<TAllocateMemoryStep>(*addIndex, entityIds, mType, stage);
    }
}

NKikimr::TConclusion<bool> TFilterCutLimit::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->MutableStageData().CutFilter(source->GetRecordsCount(), Limit, Reverse);
    return true;
}

TConclusion<bool> TPortionAccessorFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingAccessor(source, step);
}

TConclusion<bool> TDetectInMem::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    if (Columns.GetColumnsCount()) {
        source->SetSourceInMemory(
            source->GetColumnRawBytes(Columns.GetColumnIds()) < NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
    } else {
        source->SetSourceInMemory(true);
    }
    AFL_VERIFY(!source->NeedAccessorsFetching());
    auto plan = source->GetContext()->GetColumnsFetchingPlan(source);
    source->InitFetchingPlan(plan);
    TFetchingScriptCursor cursor(plan, 0);
    auto task = std::make_shared<TStepAction>(source, std::move(cursor), source->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    return false;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
