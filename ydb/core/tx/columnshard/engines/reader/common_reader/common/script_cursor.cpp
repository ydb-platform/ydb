#include "script_cursor.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <yql/essentials/minikql/mkql_terminator.h>
#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

TConclusion<bool> TFetchingScriptCursor::Execute(const std::shared_ptr<IDataSource>& source) {
    AFL_VERIFY(source);
    YDB_LOG_CREATE_CONTEXT(
        {"sourceIdx", source->GetSourceIdx()},
        {"tabletId", source->GetContext()->GetCommonContext()->GetReadMetadata()->GetTabletId()});
    NMiniKQL::TThrowingBindTerminator bind;
    if (StepStartInstant == TMonotonic::Zero()) {
        StepStartInstant = TMonotonic::Now();
    }
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source->HasStageData() && source->GetStageData().IsEmptyWithData()) {
            YDB_LOG_DEBUG("",
                {"event", "empty_data"},
                {"scanStepIdx", CurrentStepIdx});
            source->OnEmptyStageData(source);
            break;
        } else if (source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            YDB_LOG_DEBUG("",
                {"event", "empty_result"},
                {"scanStepIdx", CurrentStepIdx});
            break;
        }
        const NColumnShard::TConcreteScanCounters& counters = source->GetContext()->GetCommonContext()->GetCounters();
        if (StepEndIfStepIsAsync.has_value()) {
            // previous step was asynchronous; measure how long we waited since it finished
            auto waitDuration = TMonotonic::Now() - *StepEndIfStepIsAsync;
            if (CurrentStepIdx == 0) {
                return TConclusionStatus::Fail("Internal error: step 0 should not have a predecessor");
            }
            counters.CountersForStep(Script->GetStep(CurrentStepIdx - 1)->GetName()).WaitDurationMicroSeconds->Add(waitDuration.MicroSeconds());
        }
        auto step = Script->GetStep(CurrentStepIdx);
        std::optional<TMemoryProfileGuard> mGuard;
        if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY)) {
            mGuard.emplace("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName());
        }
        YDB_LOG_DEBUG("",
            {"scanStep", step->DebugString()},
            {"scanStepIdx", CurrentStepIdx});

        const TMonotonic startInstant = TMonotonic::Now();
        const TConclusion<bool> resultStep = step->ExecuteInplace(source, *this);
        const auto executionTime = TMonotonic::Now() - startInstant;

        counters.CountersForStep(step->GetName()).ExecutionDurationMicroSeconds->Add(executionTime.MicroSeconds());
        counters.AddExecutionDuration(executionTime);

        Script->AddStepDuration(CurrentStepIdx, executionTime, TMonotonic::Now() - StepStartInstant);
        if (resultStep.IsFail()) {
            YDB_LOG_DEBUG("",
                {"scanStep", step->DebugString()},
                {"scanStepIdx", CurrentStepIdx},
                {"error", resultStep.GetErrorMessage()});
            return resultStep;
        }
        if (!*resultStep) {
            StepEndIfStepIsAsync.emplace(TMonotonic::Now());
            YDB_LOG_DEBUG("",
                {"scanStep", step->DebugString()},
                {"scanStepIdx", CurrentStepIdx});
            return false;
        } else {
            StepEndIfStepIsAsync = std::nullopt;
        }
        StepStartInstant = TMonotonic::Now();
        ++CurrentStepIdx;
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fcursor"));
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
