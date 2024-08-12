#include "conveyor_task.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NReader {

NKikimr::TConclusionStatus IDataTasksProcessor::ITask::DoExecute(const std::shared_ptr<NConveyor::ITask>& taskPtr) {
    auto result = DoExecuteImpl();
    if (result.IsFail()) {
        NActors::TActivationContext::AsActorContext().Send(OwnerId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(result));
    } else {
        NActors::TActivationContext::AsActorContext().Send(
            OwnerId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(static_pointer_cast<IDataTasksProcessor::ITask>(taskPtr)));
    }
    return result;
}

void IDataTasksProcessor::ITask::DoOnCannotExecute(const TString& reason) {
    NActors::TActivationContext::AsActorContext().Send(
        OwnerId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(TConclusionStatus::Fail(reason)));
}

}
