#pragma once

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderElection(
    NActors::TActorId rowDispatcherId,
    NActors::TActorId coordinatorId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
