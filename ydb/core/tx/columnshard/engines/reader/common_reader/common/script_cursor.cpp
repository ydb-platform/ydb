#include "script_cursor.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NCommon {

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    NMiniKQL::TThrowingBindTerminator bind;
    if (StepStartInstant == TMonotonic::Zero()) {
        StepStartInstant = TMonotonic::Now();
    }
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->HasStageData() && source->GetStageData().IsEmptyWithData()) {
            source->OnEmptyStageData(source);
            break;
        } else if (source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            break;
        }
        auto step = Script->GetStep(CurrentStepIdx);
        std::optional<TMemoryProfileGuard> mGuard;
        if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY)) {
            mGuard.emplace("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName());
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", step->DebugString())("scan_step_idx", CurrentStepIdx)(
            "source_id", source->GetSourceId());

        auto& schedulableTask = source->GetContext()->GetCommonContext()->GetSchedulableTask();
        if (schedulableTask) {
            schedulableTask->IncreaseExtraUsage();
        }

        const TMonotonic startInstant = TMonotonic::Now();
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        const auto executionTime = TMonotonic::Now() - startInstant;
        source->GetContext()->GetCommonContext()->GetCounters().AddExecutionDuration(executionTime);

        if (schedulableTask) {
            schedulableTask->DecreaseExtraUsage(executionTime);
        }

        Script->AddStepDuration(CurrentStepIdx, executionTime, TMonotonic::Now() - StepStartInstant);
        if (!resultStep) {
            return resultStep;
        }
        if (!*resultStep) {
            return false;
        }
        StepStartInstant = TMonotonic::Now();
        ++CurrentStepIdx;
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fcursor"));
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
