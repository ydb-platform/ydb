#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ESpecialTaskCategory category, const ui64 internalProcessId,
    std::optional<TWorkloadManagerQueryIdentity> workloadManagerQueryIdentity)
    : Task(task)
    , Category(category)
    , InternalProcessId(internalProcessId)
    , WorkloadManagerQueryIdentity(std::move(workloadManagerQueryIdentity)) {
    AFL_VERIFY(Task);
}

}   // namespace NKikimr::NConveyorComposite
