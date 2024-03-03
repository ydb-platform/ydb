#pragma once

#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/signer/signer.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NKikimr  {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NFq {

NActors::IActor* CreateYqlAnalyticsPrivateProxy(
    const NConfig::TPrivateProxyConfig& privateProxyConfig,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    ::NMonitoring::TDynamicCounterPtr counters,
    const ::NFq::TSigner::TPtr& signer
);

NActors::IActor* CreatePingTaskRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvPingTaskRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

NActors::IActor* CreateGetTaskRequestActor(
    const NActors::TActorId& sender,
    const ::NFq::TSigner::TPtr& signer,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvGetTaskRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

NActors::IActor* CreateWriteTaskResultRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvWriteTaskResultRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

NActors::IActor* CreateNodesHealthCheckActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvNodesHealthCheckRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

NActors::IActor* CreateCreateRateLimiterResourceRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvCreateRateLimiterResourceRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

NActors::IActor* CreateDeleteRateLimiterResourceRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvDeleteRateLimiterResourceRequest> ev,
    ::NMonitoring::TDynamicCounterPtr counters
);

} /* NFq */
