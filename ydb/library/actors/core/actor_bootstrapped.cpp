#include "actor_bootstrapped.h"

namespace NActors::NDetail {

TAutoPtr<IEventHandle> MakeBootstrapEventHandle(const TActorId& self, const TActorId& parentId) {
    return new IEventHandle(TEvents::TSystem::Bootstrap, 0, self, parentId, {}, 0);
}

void AbortUnexpectedBootstrapMessage(const IEventHandle& ev) {
    Y_ABORT("Unexpected bootstrap message: %s", ev.GetTypeName().data());
}

} // namespace NActors::NDetail
