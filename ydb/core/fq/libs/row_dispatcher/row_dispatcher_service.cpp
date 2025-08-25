#include "row_dispatcher_service.h"
#include "actors_factory.h"

#include "row_dispatcher.h"

namespace NKikimrConfig {
class TSharedReadingConfig;
} // namespace NKikimrConfig


namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcherService(
    const NKikimrConfig::TSharedReadingConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    NActors::TActorId nodesManagerId,
    NActors::TMon* monitoring,
    ::NMonitoring::TDynamicCounterPtr countersRoot)
{
    return NewRowDispatcher(
        config,
        credentialsProviderFactory,
        yqSharedResources,
        credentialsFactory,
        tenant,
        NFq::NRowDispatcher::CreateActorFactory(),
        counters,
        countersRoot,
        pqGateway,
        nodesManagerId,
        monitoring);
}

} // namespace NFq
