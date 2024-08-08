#include "abstract.h"
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/actors/core/log.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NConveyor {
TConclusionStatus ITask::Execute(std::shared_ptr<TTaskSignals> signals, const std::shared_ptr<ITask>& taskPtr) {
    AFL_VERIFY(!ExecutedFlag);
    ExecutedFlag = true;
    const TMonotonic start = TMonotonic::Now();
    try {
        TConclusionStatus result = DoExecute(taskPtr);
        if (result.IsFail()) {
            if (signals) {
                signals->Fails->Add(1);
                signals->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
        } else {
            if (signals) {
                signals->Success->Add(1);
                signals->SuccessDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
        }
        return result;
    } catch (...) {
        if (signals) {
            signals->Fails->Add(1);
            signals->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
        }
        return TConclusionStatus::Fail("exception: " + CurrentExceptionMessage());
    }
}

void ITask::DoOnCannotExecute(const TString& reason) {
    AFL_VERIFY(false)("problem", "cannot execute conveyor task")("reason", reason);
}

}
