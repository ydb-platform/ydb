#include "constructor.h"
#include "default_fetching.h"
#include "fetch_steps.h"
#include "fetching.h"
#include "source.h"
#include "sub_columns_fetching.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>

#include <util/string/builder.h>
#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NCommon {

bool TStepAction::DoApply(IDataReader& owner) const {
    if (FinishedFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
        Source->OnSourceFetchingFinishedSafe(owner, Source);
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
        FinishedFlag = true;
    }
    return TConclusionStatus::Success();
}

TStepAction::TStepAction(const std::shared_ptr<IDataSource>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId)
    : TBase(ownerActorId)
    , Source(source)
    , Cursor(std::move(cursor))
    , CountersGuard(Source->GetContext()->GetCommonContext()->GetCounters().GetAssembleTasksGuard()) {
}

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    NMiniKQL::TThrowingBindTerminator bind;
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->HasStageData() && source->GetStageData().IsEmptyFiltered()) {
            source->OnEmptyStageData(source);
            break;
        }
        auto step = Script->GetStep(CurrentStepIdx);
        TMemoryProfileGuard mGuard("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName(),
            IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx);
        if (!CurrentStartInstant) {
            AFL_VERIFY(!CurrentStartDataSize);
            CurrentStartInstant = TMonotonic::Now();
            CurrentStartDataSize = step->GetProcessingDataSize(source);
        } else {
            AFL_VERIFY(!!CurrentStartDataSize);
        }
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        FlushDuration();
        if (!resultStep) {
            return resultStep;
        }
        if (!*resultStep) {
            return false;
        }
        ++CurrentStepIdx;
    }
    return true;
}

TString TFetchingScript::DebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    for (auto&& i : Steps) {
        sbBranch << "{" << i->DebugString() << "};";
    }
    if (!sbBranch) {
        return "";
    }
    sb << "{branch:" << BranchName << ";steps:[" << sbBranch << "]}";
    return sb;
}

TString TFetchingScript::ProfileDebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    for (auto&& i : Steps) {
        if (i->GetSumDuration() > TDuration::MilliSeconds(10)) {
            sbBranch << "{" << i->DebugString(true) << "};";
        }
    }
    if (!sbBranch) {
        return "";
    }
    sb << "{branch:" << BranchName << ";";
    if (AtomicGet(FinishInstant) && AtomicGet(StartInstant)) {
        sb << "duration:" << AtomicGet(FinishInstant) - AtomicGet(StartInstant) << ";";
    }

    sb << "steps_10Ms:[" << sbBranch << "]}";
    return sb;
}

void TFetchingScriptBuilder::AddAllocation(const std::set<ui32>& entityIds, const EStageFeaturesIndexes stage, const EMemType mType) {
    if (Steps.size() == 0) {
        AddStep(std::make_shared<TAllocateMemoryStep>(entityIds, mType, stage));
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

TString IFetchingStep::DebugString(const bool stats) const {
    TStringBuilder sb;
    sb << "name=" << Name;
    if (stats) {
        sb << ";duration=" << GetSumDuration() << ";"
           << "size=" << 1e-9 * GetSumSize();
    }
    sb << ";details={" << DoDebugString() << "};";
    return sb;
}

TFetchingScriptBuilder::TFetchingScriptBuilder(const TSpecialReadContext& context)
    : TFetchingScriptBuilder(context.GetReadMetadata()->GetResultSchema(), context.GetMergeColumns()) {
}

void TFetchingScriptBuilder::AddFetchingStep(const TColumnsSetIds& columns, const EStageFeaturesIndexes stage) {
    auto actualColumns = columns - AddedFetchingColumns;
    AddedFetchingColumns += columns;
    if (actualColumns.IsEmpty()) {
        return;
    }
    if (Steps.size() && std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps.back())) {
        TColumnsSetIds fetchingColumns = actualColumns + std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps.back())->GetColumns();
        Steps.pop_back();
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Blob);
        AddStep(std::make_shared<TColumnBlobsFetchingStep>(fetchingColumns));
    } else {
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Blob);
        AddStep(std::make_shared<TColumnBlobsFetchingStep>(actualColumns));
    }
}

void TFetchingScriptBuilder::AddAssembleStep(
    const TColumnsSetIds& columns, const TString& purposeId, const EStageFeaturesIndexes stage, const bool sequential) {
    auto actualColumns = columns - AddedAssembleColumns;
    AddedAssembleColumns += columns;
    if (actualColumns.IsEmpty()) {
        return;
    }
    auto actualSet = std::make_shared<TColumnsSet>(actualColumns.GetColumnIds(), FullSchema);
    if (sequential) {
        const auto notSequentialColumnIds = GuaranteeNotOptional->Intersect(*actualSet);
        if (notSequentialColumnIds.size()) {
            AddAllocation(notSequentialColumnIds, stage, EMemType::Raw);
            std::shared_ptr<TColumnsSet> cross = actualSet->BuildSamePtr(notSequentialColumnIds);
            AddStep(std::make_shared<TAssemblerStep>(cross, purposeId));
            *actualSet = *actualSet - *cross;
        }
        if (!actualSet->IsEmpty()) {
            AddAllocation(notSequentialColumnIds, stage, EMemType::RawSequential);
            AddStep(std::make_shared<TOptionalAssemblerStep>(actualSet, purposeId));
        }
    } else {
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Raw);
        AddStep(std::make_shared<TAssemblerStep>(actualSet, purposeId));
    }
}

TConclusion<bool> TProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    const bool started = !source->HasProgramIterator();
    auto readMeta = source->GetContext()->GetCommonContext()->GetReadMetadata();
    if (!source->HasProgramIterator()) {
//        Cerr << Program->DebugDOT() << Endl;
        NArrow::NSSA::TProcessorContext context(
            source, source->GetStageData().GetTable(), readMeta->GetLimitRobustOptional(), readMeta->IsDescSorted());
        auto visitor = std::make_shared<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>(context);
        source->SetProgramIterator(Program->BuildIterator(visitor), visitor);
        source->SetCursorStep(step);
    }
    auto iterator = source->GetProgramIteratorVerified();
    const auto& resources = source->GetStageData().GetTable();
    if (!started) {
        iterator->Next();
    }
    NArrow::NSSA::TProcessorContext context(
        source, source->GetStageData().GetTable(), readMeta->GetLimitRobustOptional(), readMeta->IsDescSorted());
    for (; iterator->IsValid();) {
        {
            auto conclusion = iterator->Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        {
            auto conclusion = source->GetExecutionVisitorVerified()->Execute();
            if (conclusion.IsFail()) {
                return conclusion;
            } else if (*conclusion == NArrow::NSSA::IResourceProcessor::EExecutionResult::InBackground) {
                return false;
            }
        }

        if (resources->GetRecordsCountActualOptional() == 0) {
            resources->Clear();
            break;
        }
    }
    AFL_DEBUG(NKikimrServices::SSA_GRAPH_EXECUTION)("graph_constructed", Program->DebugDOT(source->GetExecutionVisitorVerified()->GetExecutedIds()));

    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
