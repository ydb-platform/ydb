#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/core/protos/config.pb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NKikimrConfig::TSharedReadingConfig::TCoordinatorConfig& config,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
