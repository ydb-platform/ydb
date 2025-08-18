#pragma once
#include "script.h"

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingScriptCursor {
private:
    ui32 CurrentStepIdx = 0;
    YDB_READONLY(TMonotonic, StepStartInstant, TMonotonic::Zero());
    std::shared_ptr<TFetchingScript> Script;

public:
    TFetchingScriptCursor(const std::shared_ptr<TFetchingScript>& script, const ui32 index)
        : CurrentStepIdx(index)
        , Script(script) {
        AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
    }

    const TString& GetName() const {
        return Script->GetStep(CurrentStepIdx)->GetName();
    }

    TString DebugString() const {
        return Script->GetStep(CurrentStepIdx)->DebugString();
    }

    bool Next() {
        AFL_VERIFY(StepStartInstant != TMonotonic::Zero());
        const auto now = TMonotonic::Now();
        Script->AddStepDuration(CurrentStepIdx, TDuration::Zero(), now - StepStartInstant);
        StepStartInstant = TMonotonic::Now();
        return !Script->IsFinished(++CurrentStepIdx);
    }

    TConclusion<bool> Execute(const std::shared_ptr<IDataSource>& source);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
