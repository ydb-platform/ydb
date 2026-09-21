#include "process.h"

namespace NKikimr::NConveyorComposite {

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
