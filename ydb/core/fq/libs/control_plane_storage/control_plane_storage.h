#pragma once

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

#define CPS_LOG_N(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_NOTICE(s)
#define CPS_LOG_D(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_DEBUG(s)
#define CPS_LOG_I(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_INFO(s)
#define CPS_LOG_W(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_WARN(s)
#define CPS_LOG_E(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_ERROR(s)
#define CPS_LOG_T(s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_TRACE(s)


#define CPS_LOG_AS_N(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_NOTICE(a, s)
#define CPS_LOG_AS_D(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_DEBUG(a, s)
#define CPS_LOG_AS_I(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_INFO(a, s)
#define CPS_LOG_AS_W(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_WARN(a, s)
#define CPS_LOG_AS_E(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_ERROR(a, s)
#define CPS_LOG_AS_T(a, s) \
    LOG_YQ_CONTROL_PLANE_STORAGE_AS_TRACE(a, s)


namespace NFq {

NActors::TActorId ControlPlaneStorageServiceActorId(ui32 nodeId = 0);

NActors::IActor* CreateInMemoryControlPlaneStorageServiceActor(const NConfig::TControlPlaneStorageConfig& config);

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
