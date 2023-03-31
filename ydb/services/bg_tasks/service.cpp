#include "service.h"

namespace NKikimr::NBackgroundTasks {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcBgrdTask");
}

void TServiceOperator::Register() {
    auto* service = Singleton<TServiceOperator>();
    service->EnabledFlag.store(true);
}

bool TServiceOperator::IsEnabled() {
    auto* service = Singleton<TServiceOperator>();
    return service->EnabledFlag.load();
}

}
