#include "common.h"
#include "config.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

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
    Y_UNUSED(ExternalProcessId);
    if (ServiceActorId) {
        NActors::TActorContext::AsActorContext().Send(
            *ServiceActorId, new NConveyorComposite::TEvExecution::TEvRegisterProcess(cpuLimits, category, scopeId, InternalProcessId));
    }
}

}   // namespace NKikimr::NConveyorComposite
