#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    TYdbConnectionPtr YdbConnection;

public:
    TActorCoordinator(
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";

};

TActorCoordinator::TActorCoordinator(
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver)) {
}

void TActorCoordinator::Bootstrap()
{
    LOG_YQ_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRDCoordinator(
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(config, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
