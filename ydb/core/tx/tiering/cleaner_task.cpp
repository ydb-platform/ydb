#include "cleaner_task.h"
#include "path_cleaner.h"

namespace NKikimr::NColumnShard::NTiers {

TTaskCleanerActivity::TFactory::TRegistrator<TTaskCleanerActivity> TTaskCleanerActivity::Registrator(TTaskCleanerActivity::GetClassNameStatic());

NKikimrSchemeOp::TTaskCleaner TTaskCleanerActivity::DoSerializeToProto() const {
    NKikimrSchemeOp::TTaskCleaner result;
    result.SetPathId(PathId);
    return result;
}

bool TTaskCleanerActivity::DoDeserializeFromProto(const NKikimrSchemeOp::TTaskCleaner& protoData) {
    PathId = protoData.GetPathId();
    return true;
}

void TTaskCleanerActivity::DoExecute(NBackgroundTasks::ITaskExecutorController::TPtr controller,
    const NBackgroundTasks::TTaskStateContainer& /*state*/)
{
    TActivationContext::AsActorContext().Register(new TPathCleaner(PathId, controller));
}

}
