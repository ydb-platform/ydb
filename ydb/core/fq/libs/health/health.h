#pragma once

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/health_config.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


#define HEALTH_LOG_D(s) \
    LOG_YQ_HEALTH_DEBUG(s)
#define HEALTH_LOG_I(s) \
    LOG_YQ_HEALTH_INFO(s)
#define HEALTH_LOG_W(s) \
    LOG_YQ_HEALTH_WARN(s)
#define HEALTH_LOG_E(s) \
    LOG_YQ_HEALTH_ERROR(s)
#define HEALTH_LOG_T(s) \
    LOG_YQ_HEALTH_TRACE(s)

namespace NFq {

NActors::TActorId HealthActorId();

NActors::IActor* CreateHealthActor(
        const NConfig::THealthConfig& config,
        const NFq::TYqSharedResources::TPtr& yqSharedResources,
        const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
