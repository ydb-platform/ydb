#include "events.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyor {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const std::optional<TString>& resourcePoolKey)
    : Task(task)
    , ResourcePoolKey(resourcePoolKey) {
    AFL_VERIFY(Task);
}

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ui64 processId, const std::optional<TString>& resourcePoolKey)
    : Task(task)
    , ProcessId(processId)
    , ResourcePoolKey(resourcePoolKey)
{
    AFL_VERIFY(Task);
}

}
