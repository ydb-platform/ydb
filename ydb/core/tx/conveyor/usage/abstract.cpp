#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NConveyor {
void ITask::Execute(std::shared_ptr<TTaskSignals> signals, const std::shared_ptr<ITask>& taskPtr) {
    AFL_VERIFY(!ExecutedFlag);
    ExecutedFlag = true;
    const TMonotonic start = TMonotonic::Now();
    try {
        DoExecute(taskPtr);
        if (signals) {
            signals->Success->Add(1);
            signals->SuccessDuration->Add((TMonotonic::Now() - start).MicroSeconds());
        }
    } catch (...) {
        if (signals) {
            signals->Fails->Add(1);
            signals->FailsDuration->Add((TMonotonic::Now() - start).MicroSeconds());
        }
        AFL_ERROR(NKikimrServices::TX_CONVEYOR)("event", "exception_on_execute")("message", CurrentExceptionMessage());
        OnCannotExecute(CurrentExceptionMessage());
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
