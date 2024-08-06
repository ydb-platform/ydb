#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TAllocationGuard::~TAllocationGuard() {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishTask>(AllocationId));
    }
}

void TAllocationGuard::Update(const ui64 newVolume) {
    Memory = newVolume;
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(
            ActorId, std::make_unique<NEvents::TEvExternal::TEvUpdateTask>(AllocationId, newVolume));
    }
}

void IAllocation::OnAllocated(std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) {
    AFL_VERIFY(!Allocated);
    Allocated = true;
    AFL_VERIFY(allocation);
    AFL_VERIFY(guard);
    DoOnAllocated(std::move(guard), allocation);
}

TGroupGuard::~TGroupGuard() {
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishGroup>(GroupId));
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
