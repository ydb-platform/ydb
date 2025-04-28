#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyor {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, NKqp::NScheduler::TSchedulableTaskPtr schedulableTask)
    : Task(task)
    , SchedulableTask(schedulableTask)
{
    AFL_VERIFY(Task);
}

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ui64 processId, NKqp::NScheduler::TSchedulableTaskPtr schedulableTask)
    : Task(task)
    , ProcessId(processId)
    , SchedulableTask(schedulableTask)
{
    AFL_VERIFY(Task);
}

} // namespace NKikimr::NConveyor
