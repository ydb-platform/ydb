#include "actor.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TMemoryLimiterActor::Bootstrap() {
    Manager = std::make_shared<TManager>(SelfId(), Config, Name, Signals, DefaultStage);
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    for (auto&& i : ev->Get()->GetAllocations()) {
        Manager->RegisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId(), i,
            ev->Get()->GetStageFeaturesIdx());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    Manager->UnregisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev) {
    Manager->UpdateAllocation(
        ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetAllocationId(), ev->Get()->GetVolume());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    Manager->UnregisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev) {
    Manager->RegisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev) {
    Manager->UnregisterProcess(ev->Get()->GetExternalProcessId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev) {
    Manager->RegisterProcess(ev->Get()->GetExternalProcessId(), ev->Get()->GetStages());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcessScope::TPtr& ev) {
    Manager->UnregisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcessScope::TPtr& ev) {
    Manager->RegisterProcessScope(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalScopeId());
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
