#include "service.h"

namespace NKikimr::NBackgroundTasks {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcBgrdTask");
}

void TServiceOperator::Register() {
    Singleton<TServiceOperator>()->EnabledFlag = true;
}

bool TServiceOperator::IsEnabled() {
    return Singleton<TServiceOperator>()->EnabledFlag;
}

}
