#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NConveyor {
TConclusionStatus ITask::Execute(std::shared_ptr<TTaskCounters> counters, const std::shared_ptr<ITask>& taskPtr) {
    AFL_VERIFY(!ExecutedFlag);
    ExecutedFlag = true;
    const TMonotonic start = TMonotonic::Now();
    try {
        TConclusionStatus result = DoExecute(taskPtr);
        if (result.IsFail()) {
            if (counters) {
                counters->Fails->Add(1);
                counters->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
        } else {
            if (counters) {
                counters->Success->Add(1);
                counters->SuccessDuration->Add((TMonotonic::Now() - start).MicroSeconds());
            }
        }
        return result;
    } catch (...) {
        if (counters) {
            counters->Fails->Add(1);
            counters->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
        }
        return TConclusionStatus::Fail("exception: " + CurrentExceptionMessage());
    }
}

void ITask::DoOnCannotExecute(const TString& reason) {
    AFL_VERIFY(false)("problem", "cannot execute conveyor task")("reason", reason);
}

void TProcessGuard::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    if (ServiceActorId && NActors::TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(*ServiceActorId, new NConveyor::TEvExecution::TEvUnregisterProcess(ProcessId));
    }
}

}
