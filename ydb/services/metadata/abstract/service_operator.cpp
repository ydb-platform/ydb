#include "service_operator.h"

#include <util/generic/singleton.h>

namespace NKikimr::NMetadata::NProvider {

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

} // namespace NKikimr::NMetadata::NProvider
