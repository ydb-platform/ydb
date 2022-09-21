#pragma once

#include <ydb/core/yq/libs/actors/logging/log.h>
#include <ydb/core/yq/libs/config/protos/control_plane_proxy.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


#define CPP_LOG_D(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_DEBUG(s)
#define CPP_LOG_I(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_INFO(s)
#define CPP_LOG_W(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_WARN(s)
#define CPP_LOG_E(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_ERROR(s)
#define CPP_LOG_T(s) \
    LOG_YQ_CONTROL_PLANE_PROXY_TRACE(s)

namespace NYq {

NActors::TActorId ControlPlaneProxyActorId();

NActors::IActor* CreateControlPlaneProxyActor(const NConfig::TControlPlaneProxyConfig& config, const ::NMonitoring::TDynamicCounterPtr& counters, bool getQuotas);

} // namespace NYq
