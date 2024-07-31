#include "row_dispatcher_service.h"

#include "row_dispatcher.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcherService(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant)
{
    return NewRowDispatcher(config, commonConfig, credentialsProviderFactory, yqSharedResources, credentialsFactory, tenant);
}

} // namespace NFq
