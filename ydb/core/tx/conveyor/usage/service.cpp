#include "service.h"

namespace NKikimr::NConveyor {

bool TServiceOperator::IsEnabled() {
    return Singleton<TServiceOperator>()->IsEnabledFlag;
}

void TServiceOperator::Register(const TConfig& serviceConfig) {
    Singleton<TServiceOperator>()->IsEnabledFlag = serviceConfig.IsEnabled();
}

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcConveyor");
}

}
