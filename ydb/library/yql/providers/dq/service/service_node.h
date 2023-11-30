#pragma once

#include "grpc_service.h"
#include "interconnect_helpers.h"

#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>

#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller_actor.h>

namespace NYql {
    class TServiceNode {
    public:
        TServiceNode(
            const NDqs::TServiceNodeConfig& config,
            ui32 threads,
            IMetricsRegistryPtr metricsRegistry);

        void AddLocalService(NActors::TActorId actorId, NActors::TActorSetupCmd service);
        NActors::TActorSystem* StartActorSystem(void* appData = nullptr);
        void StartService(const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories);

        void Stop(TDuration time = TDuration::Max());

        NActors::TActorSystemSetup* GetSetup() const {
            return Setup.Get();
        }

    private:
        NDqs::TServiceNodeConfig Config;
        ui32 Threads;
        IMetricsRegistryPtr MetricsRegistry;
        THolder<NActors::TActorSystemSetup> Setup;
        TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
        THolder<NActors::TActorSystem> ActorSystem;
        TVector<NActors::TActorId> ActorIds;
        THolder<NYdbGrpc::TGRpcServer> Server;
        TIntrusivePtr<NYdbGrpc::IGRpcService> Service;
    };
}
