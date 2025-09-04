#include "storage_service.h"

#include "storage_proxy.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const NKikimrConfig::TCheckpointsConfig& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return NewStorageProxy(config, idsPrefix, credentialsProviderFactory, yqSharedResources, counters);
}

} // namespace NFq
