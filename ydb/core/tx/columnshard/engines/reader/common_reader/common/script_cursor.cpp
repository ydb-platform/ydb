#include "script_cursor.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NCommon {

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("source_id", source->GetSourceId())(
        "tablet_id", source->GetContext()->GetCommonContext()->GetReadMetadata()->GetTabletId());
    NMiniKQL::TThrowingBindTerminator bind;
    if (StepStartInstant == TMonotonic::Zero()) {
        StepStartInstant = TMonotonic::Now();
    }
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->HasStageData() && source->GetStageData().IsEmptyWithData()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "empty_data")("scan_step_idx", CurrentStepIdx);
            source->OnEmptyStageData(source);
            break;
        } else if (source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "empty_result")("scan_step_idx", CurrentStepIdx);
            break;
        }
        auto step = Script->GetStep(CurrentStepIdx);
        std::optional<TMemoryProfileGuard> mGuard;
        if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY)) {
            mGuard.emplace("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName());
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx);

        const TMonotonic startInstant = TMonotonic::Now();
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        const auto executionTime = TMonotonic::Now() - startInstant;
        source->GetContext()->GetCommonContext()->GetCounters().AddExecutionDuration(executionTime);

        Script->AddStepDuration(CurrentStepIdx, executionTime, TMonotonic::Now() - StepStartInstant);
        if (resultStep.IsFail()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx)(
                "error", resultStep.GetErrorMessage());
            return resultStep;
        }
        if (!*resultStep) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx);
            return false;
        }
        StepStartInstant = TMonotonic::Now();
        ++CurrentStepIdx;
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fcursor"));
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
