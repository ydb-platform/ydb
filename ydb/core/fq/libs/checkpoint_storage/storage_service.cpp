#include "storage_service.h"

#include "storage_proxy.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const TCheckpointStorageSettings& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return NewStorageProxy(config, idsPrefix, credentialsProviderFactory, std::move(driver), counters);
}

} // namespace NFq
