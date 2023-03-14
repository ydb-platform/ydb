#include "service.h"

#include "config.h"

namespace NKikimr::NCSIndex {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcExtIndex");
}

void TServiceOperator::Register(const TConfig& config) {
    auto* service = Singleton<TServiceOperator>();
    std::unique_lock<std::shared_mutex> lock(service->Lock);
    service->EnabledFlag = config.IsEnabled();
}

bool TServiceOperator::IsEnabled() {
    auto* service = Singleton<TServiceOperator>();
    std::shared_lock<std::shared_mutex> lock(service->Lock);
    return service->EnabledFlag;
}

}
