#include "controller.h"
#include "events.h"

namespace NKikimr::NOlap {

void TTabletDataAccessor::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    NActors::TActivationContext::Send(TabletActorId, std::make_unique<NDataAccessorControl::TEvAskDataAccessors>(request));
}

}   // namespace NKikimr::NOlap
