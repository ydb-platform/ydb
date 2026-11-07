#pragma once
#include "script.h"

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingScriptCursor {
private:
    ui32 CurrentStepIdx = 0;
    std::shared_ptr<NScanStats::TOperation> RuntimeStats;
    std::optional<ui32> PendingStepIdx;
    YDB_READONLY(TMonotonic, StepStartInstant, TMonotonic::Zero());
    std::shared_ptr<TFetchingScript> Script;
    std::optional<TMonotonic> StepEndIfStepIsAsync = std::nullopt;

public:
    TFetchingScriptCursor(const std::shared_ptr<TFetchingScript>& script, const ui32 index)
        : CurrentStepIdx(index)
        , Script(script)
    {
        AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    }

    void StartRuntimeStats(const std::shared_ptr<NScanStats::TCollector>& collector, ui64 sourceId) {
        if (collector && !RuntimeStats) {
            const auto& step = Script->GetStep(CurrentStepIdx);
            RuntimeStats = collector->Start(step->GetStatsKind(), step->GetName(), step->DebugString(),
                NScanStats::Current(), sourceId);
            if (StepStartInstant == TMonotonic::Zero()) { StepStartInstant = TMonotonic::Now(); }
        }
    }

    const std::shared_ptr<NScanStats::TOperation>& GetRuntimeStats() const { return RuntimeStats; }

    void FinishRuntimeStats(NScanStats::TStep::EOutcome outcome = NScanStats::TStep::completed) const {
        if (RuntimeStats) { RuntimeStats->Finish(outcome); }
    }

    const TString& GetName() const {
        return Script->GetStep(CurrentStepIdx)->GetName();
    }

    TString GetPrevName() const {
        if (CurrentStepIdx > 0) {
            return Script->GetStep(CurrentStepIdx - 1)->GetName();
        }
        return TString();
    }

    TString GetTracingName() const {
        if (CurrentStepIdx > 0) {
            return GetPrevName() + " - " + GetName();
        }
        return GetName();
    }

    ui32 GetStepIndex() const {
        return CurrentStepIdx;
    }

    TString DebugString() const {
        return Script->GetStep(CurrentStepIdx)->DebugString();
    }

    bool Next() {
        FinishRuntimeStats();
        RuntimeStats.reset();
        AFL_VERIFY(StepStartInstant != TMonotonic::Zero());
        const auto now = TMonotonic::Now();
        Script->AddStepDuration(CurrentStepIdx, TDuration::Zero(), now - StepStartInstant);
        StepStartInstant = TMonotonic::Now();
        return !Script->IsFinished(++CurrentStepIdx);
    }

    TConclusion<TExecutionResult> Execute(IDataSource& source);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
