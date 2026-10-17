#include "service.h"

#include <ydb/services/metadata/ds_table/config.h>

namespace NKikimr::NMetadata::NProvider {

void TServiceOperator::Register(const TConfig& config) {
    auto* service = Singleton<TServiceOperator>();
    std::unique_lock<std::shared_mutex> lock(service->Lock);
    service->EnabledFlag = true;
    service->Path = config.GetPath();
}

} // namespace NKikimr::NMetadata::NProvider
