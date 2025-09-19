#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

namespace NKikimrConfig {
class TSharedReadingConfig_TCoordinatorConfig;
} // namespace NKikimrConfig

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NKikimrConfig::TSharedReadingConfig_TCoordinatorConfig& config,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
