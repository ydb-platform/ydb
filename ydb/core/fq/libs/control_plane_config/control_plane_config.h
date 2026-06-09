#pragma once

#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::TActorId ControlPlaneConfigActorId();

NActors::IActor* CreateControlPlaneConfigActor(const ::NFq::TYqSharedResources::TPtr& yqSharedResources,
                                               const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
                                               const NConfig::TControlPlaneStorageConfig& config,
                                               const NConfig::TComputeConfig& computeConfig,
                                               const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
