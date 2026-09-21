#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ESpecialTaskCategory category, const ui64 internalProcessId)
    : Task(task)
    , Category(category)
    , InternalProcessId(internalProcessId) {
    AFL_VERIFY(Task);
}

TEvExecution::TEvRegisterProcess::TEvRegisterProcess(const TCPULimitsConfig& cpuLimits, const ESpecialTaskCategory category,
    const TString& scopeId, const ui64 internalProcessId, const std::optional<TSchedulerQueryIdentity>& schedulerQueryIdentity)
    : Category(category)
    , ScopeId(scopeId)
    , InternalProcessId(internalProcessId)
    , CPULimits(cpuLimits)
    , SchedulerQueryIdentity(schedulerQueryIdentity.value_or(TSchedulerQueryIdentity{})) {
}

}   // namespace NKikimr::NConveyorComposite
