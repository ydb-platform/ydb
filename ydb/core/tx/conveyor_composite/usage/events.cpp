#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ESpecialTaskCategory category, const ui64 internalProcessId, NKqp::NScheduler::TSchedulableTaskPtr schedulableTask)
    : Task(task)
    , Category(category)
    , InternalProcessId(internalProcessId)
    , SchedulableTask(schedulableTask)
{
    AFL_VERIFY(Task);
}

}   // namespace NKikimr::NConveyorComposite
