#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

TEvInternal::TEvTaskProcessedResult::TEvTaskProcessedResult(
    std::vector<TWorkerTaskResult>&& results, const TDuration forwardSendDuration, const ui64 workerIdx, const ui64 workersPoolId)
    : ForwardSendDuration(forwardSendDuration)
    , Results(std::move(results))
    , WorkerIdx(workerIdx)
    , WorkersPoolId(workersPoolId) {
    AFL_VERIFY(Results.size());
}

TWorkerTaskResult::TWorkerTaskResult(const TWorkerTaskContext& context, const TMonotonic start, const TMonotonic finish, ITask::TPtr task)
    : TBase(context)
    , Start(start)
    , Finish(finish)
    , Task(std::move(task)) {
    AFL_VERIFY(Start <= Finish);
    AFL_VERIFY(Task);
}

}   // namespace NKikimr::NConveyorComposite
