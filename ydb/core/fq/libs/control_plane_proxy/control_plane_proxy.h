#pragma once

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_proxy.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/signer/signer.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::TActorId ControlPlaneProxyActorId();

NActors::IActor* CreateControlPlaneProxyActor(
    const NConfig::TControlPlaneProxyConfig& config,
    const NConfig::TControlPlaneStorageConfig& storageConfig,
    const NConfig::TComputeConfig& computeConfig,
    const NConfig::TCommonConfig& commonConfig,
    const NYql::TS3GatewayConfig& s3Config,
    const ::NFq::TSigner::TPtr& signer,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    bool quotaManagerEnabled);

} // namespace NFq
