#include "actor.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TMemoryLimiterActor::Bootstrap() {
    Manager = std::make_shared<TManager>(SelfId(), Config, Name, Signals);
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    for (auto&& i : ev->Get()->GetAllocations()) {
        Manager->RegisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalGroupId(), i, ev->Get()->GetStageFeatures());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    Manager->UnregisterAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev) {
    Manager->UpdateAllocation(ev->Get()->GetExternalProcessId(), ev->Get()->GetAllocationId(), ev->Get()->GetVolume());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    Manager->UnregisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartGroup::TPtr& ev) {
    Manager->RegisterGroup(ev->Get()->GetExternalProcessId(), ev->Get()->GetExternalGroupId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishProcess::TPtr& ev) {
    Manager->UnregisterProcess(ev->Get()->GetExternalProcessId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartProcess::TPtr& ev) {
    Manager->RegisterProcess(ev->Get()->GetExternalProcessId());
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
