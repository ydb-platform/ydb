#include "storage_proxy.h"
#include "storage_service.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const TCheckpointStorageSettings& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    TCheckpointProviderIntegrations checkpointProviderIntegrations)
{
    return NewStorageProxy(config, idsPrefix, credentialsProviderFactory, std::move(driver), counters, std::move(checkpointProviderIntegrations));
}

} // namespace NFq
