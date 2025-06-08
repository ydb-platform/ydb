#include "common.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

void TProcessGuard::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    if (ServiceActorId && NActors::TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(*ServiceActorId, new TEvExecution::TEvUnregisterProcess(Category, ScopeId, InternalProcessId));
    }
}

}   // namespace NKikimr::NConveyorComposite
