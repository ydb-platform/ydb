#pragma once

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/actors/core/log.h>

namespace NFq {

NActors::TActorId ControlPlaneStorageServiceActorId(ui32 nodeId = 0);

NActors::IActor* CreateInMemoryControlPlaneStorageServiceActor(
    const NConfig::TControlPlaneStorageConfig& config,
    const NYql::TS3GatewayConfig& s3Config,
    const NConfig::TCommonConfig& common,
    const NConfig::TComputeConfig& computeConfig,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const TString& tenantName);

NActors::IActor* CreateYdbControlPlaneStorageServiceActor(
    const NConfig::TControlPlaneStorageConfig& config,
    const NYql::TS3GatewayConfig& s3Config,
    const NConfig::TCommonConfig& common,
    const NConfig::TComputeConfig& computeConfig,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NFq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TString& tenantName);

} // namespace NFq
