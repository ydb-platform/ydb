#pragma once
#include <ydb/core/protos/grpc.grpc.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/public/lib/base/defs.h>
#include <ydb/public/lib/base/msgbus.h>

#include <util/thread/factory.h>
#include <util/generic/queue.h>

namespace NKikimr {

namespace NMsgBusProxy {
    class TBusMessageContext;
} // NMsgBusProxy

namespace NGRpcProxy {

//! Implements interaction Kikimr via gRPC protocol.
class TGRpcService
    : public NYdbGrpc::TGrpcServiceBase<NKikimrClient::TGRpcServer>
{
public:
    TGRpcService(NActors::TActorId grpcRequestProxyId);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override final;

    NThreading::TFuture<void> Prepare(NActors::TActorSystem* system, const NActors::TActorId& pqMeta, const NActors::TActorId& msgBusProxy, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);
    void Start();

    bool IncRequest() override final;
    void DecRequest() override final;
    i64 GetCurrentInFlight() const;

private:
    void RegisterRequestActor(NActors::IActor* req);

    //! Setup handlers for incoming requests.
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    using IThreadRef = TAutoPtr<IThreadFactory::IThread>;


    NActors::TActorSystem* ActorSystem_;
    NActors::TActorId PQMeta_;
    NActors::TActorId MsgBusProxy_;

    grpc::ServerCompletionQueue* CQ_ = nullptr;
    NYdbGrpc::TLoggerPtr Logger_;

    size_t PersQueueWriteSessionsMaxCount_ = 1000000;
    size_t PersQueueReadSessionsMaxCount_  = 100000;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;

    std::function<void()> InitCb_;
    // In flight request management.
    NYdbGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

}
}
