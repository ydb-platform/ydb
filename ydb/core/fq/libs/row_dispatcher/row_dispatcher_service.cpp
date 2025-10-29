#include "row_dispatcher_service.h"
#include "actors_factory.h"

#include "row_dispatcher.h"

namespace NFq {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcherService(
    const TRowDispatcherSettings& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    NYdb::TDriver driver,
    NActors::TMon* monitoring,
    ::NMonitoring::TDynamicCounterPtr countersRoot,
    NActors::TActorId nodesManagerId)
{
    return NewRowDispatcher(
        config,
        credentialsProviderFactory,
        credentialsFactory,
        tenant,
        NFq::NRowDispatcher::CreateActorFactory(),
        functionRegistry,
        counters,
        countersRoot,
        pqGateway,
        driver,
        monitoring,
        nodesManagerId);
}

} // namespace NFq
