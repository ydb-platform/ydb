#include "service.h"

#include <ydb/services/metadata/ds_table/config.h>

namespace NKikimr::NMetadata::NProvider {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

void TServiceOperator::Register(const TConfig& config) {
    Singleton<TServiceOperator>()->EnabledFlag = true;
    Singleton<TServiceOperator>()->Path = config.GetPath();
}

bool TServiceOperator::IsEnabled() {
    return Singleton<TServiceOperator>()->EnabledFlag;
}

const TString& TServiceOperator::GetPath() {
    return Singleton<TServiceOperator>()->Path;
}

}
