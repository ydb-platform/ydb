#pragma once

//#include "checkpoint_storage.h"
//#include "state_storage.h"

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

//#include <memory>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant);

} // namespace NFq
