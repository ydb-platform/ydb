#include "process.h"

namespace NKikimr::NConveyorComposite {

void TProcess::MoveToServiceQuery() {
    Y_ENSURE(!GetInProgressTasksCount(), "cannot migrate a process with in-progress tasks");
    SchedulerQueryIdentity = kServiceQueryIdentity;
}

TProcess::TProcess(const ui64 processId, const std::shared_ptr<TProcessScope>& scope,
    const std::shared_ptr<TPositiveControlInteger>& waitingTasksCount, const TSchedulerQueryIdentity& schedulerQueryIdentity)
    : ProcessId(processId)
    , Scope(scope)
    , SchedulerQueryIdentity(schedulerQueryIdentity)
    , WaitingTasksCount(waitingTasksCount) {
    AFL_VERIFY(WaitingTasksCount);
    CPUUsage = std::make_shared<TCPUUsage>(Scope->GetCPUUsage());
}

}
