#include "fetch_steps.h"
#include "fetching.h"
#include "source.h"

#include <util/string/builder.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

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
        if (source->HasStageData() && source->GetStageData().IsEmpty()) {
            source->OnEmptyStageData(source);
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
    sb << "{branch:" << BranchName << ";";
    if (AtomicGet(FinishInstant) && AtomicGet(StartInstant)) {
        sb << "duration:" << AtomicGet(FinishInstant) - AtomicGet(StartInstant) << ";";
    }

    sb << "steps_10Ms:[" << sbBranch << "]}";
    return sb;
}

TFetchingScript::TFetchingScript(const TSpecialReadContext& /*context*/) {
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

TString IFetchingStep::DebugString() const {
    TStringBuilder sb;
    sb << "name=" << Name << ";duration=" << GetSumDuration() << ";"
       << "size=" << 1e-9 * GetSumSize() << ";details={" << DoDebugString() << "};";
    return sb;
}

bool TColumnsAccumulator::AddFetchingStep(TFetchingScript& script, const TColumnsSetIds& columns, const EStageFeaturesIndexes stage) {
    auto actualColumns = GetNotFetchedAlready(columns);
    FetchingReadyColumns = FetchingReadyColumns + (TColumnsSetIds)columns;
    if (!actualColumns.IsEmpty()) {
        script.Allocation(columns.GetColumnIds(), stage, EMemType::Blob);
        script.AddStep(std::make_shared<TColumnBlobsFetchingStep>(actualColumns));
        return true;
    }
    return false;
}

bool TColumnsAccumulator::AddAssembleStep(
    TFetchingScript& script, const TColumnsSetIds& columns, const TString& purposeId, const EStageFeaturesIndexes stage, const bool sequential) {
    auto actualColumns = columns - AssemblerReadyColumns;
    AssemblerReadyColumns = AssemblerReadyColumns + columns;
    if (actualColumns.IsEmpty()) {
        return false;
    }
    auto actualSet = std::make_shared<TColumnsSet>(actualColumns.GetColumnIds(), FullSchema);
    if (sequential) {
        const auto notSequentialColumnIds = GuaranteeNotOptional->Intersect(*actualSet);
        if (notSequentialColumnIds.size()) {
            script.Allocation(notSequentialColumnIds, stage, EMemType::Raw);
            std::shared_ptr<TColumnsSet> cross = actualSet->BuildSamePtr(notSequentialColumnIds);
            script.AddStep<TAssemblerStep>(cross, purposeId);
            *actualSet = *actualSet - *cross;
        }
        if (!actualSet->IsEmpty()) {
            script.Allocation(notSequentialColumnIds, stage, EMemType::RawSequential);
            script.AddStep<TOptionalAssemblerStep>(actualSet, purposeId);
        }
    } else {
        script.Allocation(actualColumns.GetColumnIds(), stage, EMemType::Raw);
        script.AddStep<TAssemblerStep>(actualSet, purposeId);
    }
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
