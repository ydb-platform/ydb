#include "common.h"
#include "config.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

bool TProcessGuard::SendTaskToExecute(const std::shared_ptr<ITask>& task,
    const std::optional<TWorkloadManagerQueryIdentity>& workloadManagerQueryIdentity) const {
    AFL_VERIFY(!Finished);
    if (ServiceActorId && NActors::TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(*ServiceActorId,
            new TEvExecution::TEvNewTask(task, Category, InternalProcessId, workloadManagerQueryIdentity));
        return true;
    } else {
        task->Execute(nullptr, task);
        return false;
    }
}

void TProcessGuard::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    if (ServiceActorId && NActors::TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(*ServiceActorId, new TEvExecution::TEvUnregisterProcess(Category, InternalProcessId));
    }
}

TProcessGuard::TProcessGuard(const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId,
    const TCPULimitsConfig& cpuLimits, const std::optional<NActors::TActorId>& actorId)
    : Category(category)
    , ScopeId(scopeId)
    , ExternalProcessId(externalProcessId)
    , ServiceActorId(actorId) {
    if (ServiceActorId) {
        NActors::TActorContext::AsActorContext().Send(
            *ServiceActorId, new NConveyorComposite::TEvExecution::TEvRegisterProcess(cpuLimits, category, scopeId, InternalProcessId));
    }
}

void TWorkloadManagerQueryGuard::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    if (ServiceActorId && NActors::TlsActivationContext) {
        NActors::TActorContext::AsActorContext().Send(
            *ServiceActorId, new TEvExecution::TEvUnregisterWorkloadManagerQuery(Identity));
    }
}

TWorkloadManagerQueryGuard::TWorkloadManagerQueryGuard(
    TWorkloadManagerQueryIdentity identity, const std::optional<NActors::TActorId>& actorId)
    : Identity(std::move(identity))
    , ServiceActorId(actorId) {
    if (ServiceActorId) {
        NActors::TActorContext::AsActorContext().Send(
            *ServiceActorId, new TEvExecution::TEvRegisterWorkloadManagerQuery(Identity));
    }
}

}   // namespace NKikimr::NConveyorComposite
