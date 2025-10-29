#pragma once

#include <ydb/core/fq/libs/row_dispatcher/common/row_dispatcher_settings.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/library/actors/core/actor.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const TRowDispatcherSettings::TCoordinatorSettings& config,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    NActors::TActorId nodesManagerId);

} // namespace NFq
