#pragma once

#include <contrib/ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>

#include <contrib/ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <contrib/ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <contrib/ydb/library/grpc/server/grpc_request.h>
#include <contrib/ydb/library/grpc/server/grpc_server.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/future/future.h>

#include "grpc_session.h"

namespace NYql::NDqs {
    class TDatabaseManager;

    class TDqsGrpcService: public NYdbGrpc::TGrpcServiceBase<Yql::DqsProto::DqService> {
    public:
        TDqsGrpcService(NActors::TActorSystem& system,
                        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                        const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories);

        void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;

        NThreading::TFuture<void> Stop();

    private:
        NActors::TActorSystem& ActorSystem;
        grpc::ServerCompletionQueue* CQ = nullptr;

        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        TDqTaskPreprocessorFactoryCollection DqTaskPreprocessorFactories;
        TMutex Mutex;
        NThreading::TPromise<void> Promise;
        std::atomic<ui64> RunningRequests;
        std::atomic<bool> Stopping;

        TSessionStorage Sessions;
    };
}
