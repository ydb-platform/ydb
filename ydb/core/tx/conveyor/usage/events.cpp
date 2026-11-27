#include "events.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyor {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task)
    : Task(task) {
    YDB_LOG_VERIFY(Task);
}

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ui64 processId)
    : Task(task)
    , ProcessId(processId)
{
    YDB_LOG_VERIFY(Task);
}

}
