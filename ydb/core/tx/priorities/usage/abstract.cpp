#include "abstract.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPrioritiesQueue {

TAllocationGuard::~TAllocationGuard() {
    AFL_VERIFY(Released);
}

void TAllocationGuard::Release(const NActors::TActorId& serviceActorId) {
    AFL_VERIFY(!Released);
    auto& context = NActors::TActorContext::AsActorContext();
    context.Send(serviceActorId, new TEvExecution::TEvFree(ClientId, Count));
    Released = true;
}

}   // namespace NKikimr::NPrioritiesQueue
