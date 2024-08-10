#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TAllocationGuard::~TAllocationGuard() {
    if (TlsActivationContext && !Released) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishTask>(ProcessId, AllocationId));
    }
}

void TAllocationGuard::Update(const ui64 newVolume) {
    AFL_VERIFY(!Released);
    Memory = newVolume;
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(
            ActorId, std::make_unique<NEvents::TEvExternal::TEvUpdateTask>(ProcessId, AllocationId, newVolume));
    }
}

bool IAllocation::OnAllocated(std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) {
    AFL_VERIFY(!Allocated);
    Allocated = true;
    AFL_VERIFY(allocation);
    AFL_VERIFY(guard);
    return DoOnAllocated(std::move(guard), allocation);
}

TGroupGuard::~TGroupGuard() {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishGroup>(ProcessId, GroupId));
    }
}

TGroupGuard::TGroupGuard(const NActors::TActorId& actorId, const ui64 processId, const ui64 groupId)
    : ActorId(actorId)
    , ProcessId(processId)
    , GroupId(groupId) {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvStartGroup>(ProcessId, GroupId));
    }
}

TProcessGuard::~TProcessGuard() {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishProcess>(ProcessId));
    }
}

TProcessGuard::TProcessGuard(const NActors::TActorId& actorId, const ui64 processId, const std::vector<std::shared_ptr<TStageFeatures>>& stages)
    : ActorId(actorId)
    , ProcessId(processId) {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvStartProcess>(ProcessId, stages));
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
