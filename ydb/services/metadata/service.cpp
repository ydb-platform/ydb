#include "service.h"

namespace NKikimr::NMetadataProvider {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

void TServiceOperator::Register() {
    Singleton<TServiceOperator>()->EnabledFlag = true;
}

bool TServiceOperator::IsEnabled() {
    return Singleton<TServiceOperator>()->EnabledFlag;
}

}
