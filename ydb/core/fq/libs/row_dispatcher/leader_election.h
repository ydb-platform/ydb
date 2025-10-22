#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

namespace NKikimrConfig {
class TSharedReadingConfig_TCoordinatorConfig;
} // namespace NKikimrConfig

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderElection(
    NActors::TActorId rowDispatcherId,
    NActors::TActorId coordinatorId,
    const NKikimrConfig::TSharedReadingConfig_TCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
