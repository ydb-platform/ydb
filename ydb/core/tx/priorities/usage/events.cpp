#include "events.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPrioritiesQueue {

TEvExecution::TEvAsk::TEvAsk(const ui64 clientId, const ui32 count, const std::shared_ptr<IRequest>& request, const ui64 priority)
    : ClientId(clientId)
    , Count(count)
    , Request(request)
    , Priority(priority) {
    AFL_VERIFY(Request);
    AFL_VERIFY(Count);
}

 TEvExecution::TAllocationGuard::~TAllocationGuard() {
    AFL_VERIFY(Released);
}

 void TEvExecution::TAllocationGuard::Release() {
     AFL_VERIFY(!Released);
     auto& context = NActors::TActorContext::AsActorContext();
     context.Send(ServiceActorId, new TEvFree(ClientId, Count));
     Released = true;
 }

}   // namespace NKikimr::NPrioritiesQueue
