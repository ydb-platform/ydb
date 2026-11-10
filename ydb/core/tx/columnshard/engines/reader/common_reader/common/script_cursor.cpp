#include "script_cursor.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#include <yql/essentials/minikql/mkql_terminator.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

TConclusion<TExecutionResult> TFetchingScriptCursor::Execute(IDataSource& source) {
    YDB_LOG_CREATE_CONTEXT(
        {"sourceIdx", source.GetSourceIdx()},
        {"tabletId", source.GetContext()->GetCommonContext()->GetReadMetadata()->GetTabletId()});
    NMiniKQL::TThrowingBindTerminator bind;
    if (StepStartInstant == TMonotonic::Zero()) {
        StepStartInstant = TMonotonic::Now();
    }
    Script->OnExecute();
    AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    while (!Script->IsFinished(CurrentStepIdx)) {
        if (source.HasStageData() && source.GetStageData().IsEmptyWithData()) {
            YDB_LOG_DEBUG("",
                {"event", "empty_data"},
                {"scanStepIdx", CurrentStepIdx});
            FinishRuntimeStats();
            if (source.GetExecutionContext().GetRuntimeStats()) { source.GetExecutionContext().GetRuntimeStats()->Finish(); }
            source.OnEmptyStageData();
            break;
        } else if (source.HasStageResult() && source.GetStageResult().IsEmpty()) {
            FinishRuntimeStats();
            if (source.GetExecutionContext().GetRuntimeStats()) { source.GetExecutionContext().GetRuntimeStats()->Finish(); }
            YDB_LOG_DEBUG("",
                {"event", "empty_result"},
                {"scanStepIdx", CurrentStepIdx});
            break;
        }
        const NColumnShard::TConcreteScanCounters& counters = source.GetContext()->GetCommonContext()->GetCounters();
        if (StepEndIfStepIsAsync.has_value()) {
            // previous step was asynchronous; measure how long we waited since it finished
            auto waitDuration = TMonotonic::Now() - *StepEndIfStepIsAsync;
            counters.CountersForStep(Script->GetStep(PendingStepIdx.value_or(CurrentStepIdx))->GetName())
                .WaitDurationMicroSeconds->Add(waitDuration.MicroSeconds());
            StepEndIfStepIsAsync.reset();
        }
        auto step = Script->GetStep(CurrentStepIdx);
        std::optional<TMemoryProfileGuard> mGuard;
        if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY)) {
            mGuard.emplace("SCAN_PROFILE::FETCHING::" + step->GetName() + "::" + Script->GetBranchName());
        }
        YDB_LOG_DEBUG("",
            {"scanStep", step->DebugString()},
            {"scanStepIdx", CurrentStepIdx});

        StartRuntimeStats(source.GetContext()->GetCommonContext()->GetRuntimeStats(), source.GetSourceId());
        if (RuntimeStats) {
            TString meta = step->GetStatsMetaStep();
            if (!meta && step->GetName() != "PROGRAM_EXECUTION") {
                if (step->GetStatsKind() == NScanStats::TStep::BuildResult ||
                    step->GetStatsKind() == NScanStats::TStep::PrepareResult) {
                    const auto& collector = source.GetContext()->GetCommonContext()->GetRuntimeStats();
                    meta = collector && collector->GetReaderKind() == "PLAIN" ? "PKFilter" : "BuildResult";
                } else {
                    meta = "PKFilter";
                }
            }
            RuntimeStats->Update([&](NScanStats::TStep& record) {
                if (meta) {
                    record.SetMetaStep(meta);
                    record.SetCanonicalPath("");
                } else {
                    record.ClearMetaStep();
                    record.ClearCanonicalPath();
                }
            });
        }
        if (RuntimeStats && step->GetStatsKind() == NScanStats::TStep::Filter) {
            const auto rows = source.GetStatsCandidateRows();
            RuntimeStats->Update([&](NScanStats::TStep& record) {
                auto* details = record.MutableDetails()->MutableFilter();
                if (!details->HasInputRows() && rows) { details->SetInputRows(*rows); }
                if (step->GetStatsPurpose()) { details->SetPurpose(*step->GetStatsPurpose()); }
            });
        }
        PendingStepIdx = CurrentStepIdx;
        const TMonotonic startInstant = TMonotonic::Now();
        auto resultStep = [&] {
            NScanStats::TScope scope(RuntimeStats, source.GetContext()->GetCommonContext()->GetRuntimeStats());
            return step->ExecuteInplace(source, *this);
        }();
        const auto executionTime = TMonotonic::Now() - startInstant;

        counters.CountersForStep(step->GetName()).ExecutionDurationMicroSeconds->Add(executionTime.MicroSeconds());
        counters.AddExecutionDuration(executionTime);

        Script->AddStepDuration(CurrentStepIdx, executionTime, TMonotonic::Now() - StepStartInstant);
        if (resultStep.IsFail()) {
            FinishRuntimeStats(NScanStats::TStep::failed);
            YDB_LOG_DEBUG("",
                {"scanStep", step->DebugString()},
                {"scanStepIdx", CurrentStepIdx},
                {"error", resultStep.GetErrorMessage()});
            return resultStep;
        }
        if (resultStep->IsPending()) {
            if (RuntimeStats) { RuntimeStats->Suspend(); }
            StepEndIfStepIsAsync.emplace(TMonotonic::Now());
            YDB_LOG_DEBUG("",
                {"scanStep", step->DebugString()},
                {"scanStepIdx", CurrentStepIdx});
            return resultStep;
        }
        if (RuntimeStats && step->GetStatsKind() == NScanStats::TStep::Filter) {
            const auto rows = source.GetStatsCandidateRows();
            if (rows) { RuntimeStats->Update([&](NScanStats::TStep& record) { record.MutableDetails()->MutableFilter()->SetOutputRows(*rows); }); }
        }
        FinishRuntimeStats();
        RuntimeStats.reset();
        StepEndIfStepIsAsync = std::nullopt;
        StepStartInstant = TMonotonic::Now();
        ++CurrentStepIdx;
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("fcursor"));
    return TExecutionResult::Done();
}

}   // namespace NKikimr::NOlap::NReader::NCommon
