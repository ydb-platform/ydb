#include "storage_service.h"

#include "storage_proxy.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return NewStorageProxy(config, commonConfig, credentialsProviderFactory, yqSharedResources, counters);
}

} // namespace NFq
