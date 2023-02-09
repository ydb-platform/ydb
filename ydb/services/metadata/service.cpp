#include "service.h"

#include <ydb/services/metadata/ds_table/config.h>

namespace NKikimr::NMetadata::NProvider {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

void TServiceOperator::Register(const TConfig& config) {
    auto* service = Singleton<TServiceOperator>();
    std::unique_lock<std::shared_mutex> lock(service->Lock);
    service->EnabledFlag = true;
    service->Path = config.GetPath();
}

bool TServiceOperator::IsEnabled() {
    auto* service = Singleton<TServiceOperator>();
    std::shared_lock<std::shared_mutex> lock(service->Lock);
    return service->EnabledFlag;
}

TString TServiceOperator::GetPath() {
    auto* service = Singleton<TServiceOperator>();
    std::shared_lock<std::shared_mutex> lock(service->Lock);
    return service->Path;
}

}
