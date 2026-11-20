#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPrioritiesQueue {

TAllocationGuard::~TAllocationGuard() {
    if (!Released) {
        Release();
    }
}

void TAllocationGuard::Release() {
    YDB_LOG_VERIFY(!Released);
    if (TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(ServiceActorId, new TEvExecution::TEvFree(ClientId, Count));
    }
    Released = true;
}

}   // namespace NKikimr::NPrioritiesQueue
