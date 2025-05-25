#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

TEvExecution::TEvNewTask::TEvNewTask(ITask::TPtr task, const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId)
    : Task(task)
    , Category(category)
    , ScopeId(scopeId)
    , ProcessId(processId) {
    AFL_VERIFY(Task);
}

}   // namespace NKikimr::NConveyorComposite
