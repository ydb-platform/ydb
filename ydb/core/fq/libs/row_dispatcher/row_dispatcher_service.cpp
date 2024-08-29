#include "row_dispatcher_service.h"
#include "actors_factory.h"

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
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return NewRowDispatcher(
        config,
        commonConfig,
        credentialsProviderFactory,
        yqSharedResources,
        credentialsFactory,
        tenant,
        NFq::NRowDispatcher::CreateActorFactory(),
        counters);
}

} // namespace NFq
