#include "controller.h"
namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

void TCollector::DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    NActors::TActivationContext::Send(TabletActorId, std::make_unique<NDataAccessorControl::TEvAskDataAccessors>(request));
}

}   // namespace NKikimr::NOlap
