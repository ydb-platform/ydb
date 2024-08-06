#include "actor.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TMemoryLimiterActor::Bootstrap() {
    Manager = std::make_shared<TManager>(SelfId(), Config, Name, Signals);
    Become(&TThis::StateWait);
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvStartTask::TPtr& ev) {
    for (auto&& i : ev->Get()->GetAllocations()) {
        Manager->RegisterAllocation(i, ev->Get()->GetExternalGroupId());
    }
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishTask::TPtr& ev) {
    Manager->UnregisterAllocation(ev->Get()->GetAllocationId());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvUpdateTask::TPtr& ev) {
    Manager->UpdateAllocation(ev->Get()->GetAllocationId(), ev->Get()->GetVolume());
}

void TMemoryLimiterActor::Handle(NEvents::TEvExternal::TEvFinishGroup::TPtr& ev) {
    Manager->UnregisterGroup(ev->Get()->GetExternalGroupId());
}

}
