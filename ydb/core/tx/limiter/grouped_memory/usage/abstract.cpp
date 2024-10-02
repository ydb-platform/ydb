#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TAllocationGuard::~TAllocationGuard() {
    if (Released || !IsRegistered) {
        return;
    }
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(
            ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishTask>(ProcessId, ScopeId, AllocationId));
    } else {
        AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("failed to unregister allocation", AllocationId);
    }
}

void TAllocationGuard::Update(const ui64 newVolume) {
    AFL_VERIFY(!Released);
    Memory = newVolume;
    if (TlsActivationContext) {
        IsRegistered = true;
        NActors::TActivationContext::AsActorContext().Send(
            ActorId, std::make_unique<NEvents::TEvExternal::TEvUpdateTask>(ProcessId, ScopeId, AllocationId, newVolume));
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
    if (!IsRegistered) {
        return;
    }
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishGroup>(ProcessId, ExternalScopeId, GroupId));
    } else {
        AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("failed to unregister group", GroupId);
    }
}

TGroupGuard::TGroupGuard(const NActors::TActorId& actorId, const ui64 processId, const ui64 externalScopeId, const ui64 groupId)
    : ActorId(actorId)
    , ProcessId(processId)
    , ExternalScopeId(externalScopeId)
    , GroupId(groupId) {
    if (TlsActivationContext) {
        IsRegistered = true;
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvStartGroup>(ProcessId, ExternalScopeId, GroupId));
    }
}

TProcessGuard::~TProcessGuard() {
    if (!IsRegistered) {
        return;
    }
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishProcess>(ProcessId));
    } else {
        AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("failed to unregister process", ProcessId);
    }
}

TProcessGuard::TProcessGuard(const NActors::TActorId& actorId, const ui64 processId, const std::vector<std::shared_ptr<TStageFeatures>>& stages)
    : ActorId(actorId)
    , ProcessId(processId) {
    if (TlsActivationContext) {
        IsRegistered = true;
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvStartProcess>(ProcessId, stages));
    }
}

TScopeGuard::~TScopeGuard() {
    if (!IsRegistered) {
        return;
    }
    if (TlsActivationContext) {
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvFinishProcessScope>(ProcessId, ScopeId));
    } else {
        AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("failed to unregister scope", ScopeId);
    }
}

TScopeGuard::TScopeGuard(const NActors::TActorId& actorId, const ui64 processId, const ui64 scopeId)
    : ActorId(actorId)
    , ProcessId(processId)
    , ScopeId(scopeId) {
    if (TlsActivationContext) {
        IsRegistered = true;
        NActors::TActivationContext::AsActorContext().Send(ActorId, std::make_unique<NEvents::TEvExternal::TEvStartProcessScope>(ProcessId, ScopeId));
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
