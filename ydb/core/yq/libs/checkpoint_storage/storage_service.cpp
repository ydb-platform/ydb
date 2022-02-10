#include "storage_service.h"

#include "storage_proxy.h"

namespace NYq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCheckpointStorageService( 
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) 
{ 
    return NewStorageProxy(config, commonConfig, credentialsProviderFactory);
}

} // namespace NYq
