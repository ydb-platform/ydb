#pragma once

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


#define CPC_LOG_D(s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_DEBUG(s)
#define CPC_LOG_I(s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_INFO(s)
#define CPC_LOG_W(s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_WARN(s)
#define CPC_LOG_E(s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_ERROR(s)
#define CPC_LOG_T(s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_TRACE(s)

#define CPC_LOG_AS_D(as, s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_AS_DEBUG(as, s)
#define CPC_LOG_AS_I(as, s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_AS_INFO(as, s)
#define CPC_LOG_AS_W(as, s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_AS_WARN(as, s)
#define CPC_LOG_AS_E(as, s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_AS_ERROR(as, s)
#define CPC_LOG_AS_T(as, s) \
    LOG_FQ_CONTROL_PLANE_CONFIG_AS_TRACE(as, s)

namespace NFq {

NActors::TActorId ControlPlaneConfigActorId();

NActors::IActor* CreateControlPlaneConfigActor(const ::NFq::TYqSharedResources::TPtr& yqSharedResources,
                                               const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
                                               const NConfig::TControlPlaneStorageConfig& config,
                                               const NConfig::TComputeConfig& computeConfig,
                                               const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
