#pragma once

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

//#include <memory>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderDetector(
    NActors::TActorId parentId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant);

} // namespace NFq
