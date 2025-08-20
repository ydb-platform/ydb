#include "grpc_server.h"
#include "grpc_proxy_status.h"

#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/legacy/service_legacy.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_request.h>
#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/library/grpc/server/grpc_async_ctx_base.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/join.h>

#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/server.h>

using grpc::Server;
using grpc::ServerContext;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerAsyncWriter;
using grpc::Status;
using grpc::StatusCode;
using grpc::ServerCompletionQueue;
using grpc::CompletionQueue;

using NKikimrClient::TResponse;
using NKikimrClient::TPersQueueRequest;

using NYdbGrpc::IQueueEvent;

using namespace NActors;
using namespace NThreading;

namespace NKikimr {
namespace NGRpcProxy {

TGRpcService::TGRpcService(NActors::TActorId grpcRequestProxyId)
    : ActorSystem(nullptr)
    , GRpcRequestProxyId(grpcRequestProxyId)
{}

void TGRpcService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    Logger = std::move(logger);
    Y_ASSERT(InitCb_);
    InitCb_();
}

TFuture<void> TGRpcService::Prepare(TActorSystem* system, const TActorId& pqMeta, const TActorId& msgBusProxy,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    auto promise = NewPromise<void>();
    InitCb_ = [=]() mutable {
        try {
            ActorSystem = system;
            PQMeta = pqMeta;
            MsgBusProxy = msgBusProxy;
            Counters = counters;

            promise.SetValue();
        } catch (...) {
            promise.SetException(std::current_exception());
        }
    };
    return promise.GetFuture();
}

void TGRpcService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter *limiter) {
    Limiter_ = limiter;
}

bool TGRpcService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

i64 TGRpcService::GetCurrentInFlight() const {
    return Limiter_->GetCurrentInFlight();
}

void TGRpcService::Start() {
    Y_ABORT_UNLESS(ActorSystem);
    ui32 nodeId = ActorSystem->NodeId;
    ActorSystem->Send(MakeGRpcProxyStatusID(nodeId), new TEvGRpcProxyStatus::TEvSetup(true, PersQueueWriteSessionsMaxCount,
                                        PersQueueReadSessionsMaxCount));
    SetupIncomingRequests(Logger);
}

void TGRpcService::RegisterRequestActor(NActors::IActor* req) {
    ActorSystem->Register(req, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
}

void TGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {

    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    using namespace ::NKikimr::NGRpcService;
    using namespace ::NKikimr::NGRpcService::NLegacyGrpcService;

#define SETUP_SERVER_METHOD(methodName, In, Out, createActorCb, rlMode, requestType, counterName, auditMode) \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                                \
        NKikimrClient::In,                                                                                   \
        NKikimrClient::Out,                                                                                  \
        TGRpcService>>                                                                                       \
    (                                                                                                        \
        this,                                                                                                \
        &Service_,                                                                                           \
        CQ,                                                                                                  \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                      \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                               \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestNoOperationCall<                           \
                NKikimrClient::In,                                                                           \
                NKikimrClient::Out,                                                                          \
                NLegacyGrpcService::TLegacyGrpcMethodAccessorTraits<NKikimrClient::In, NKikimrClient::Out>>( \
                    reqCtx, createActorCb,                                                                   \
                    TRequestAuxSettings {                                                                    \
                        .RlMode = TRateLimiterMode::rlMode,                                                  \
                        .AuditMode = auditMode,                                                              \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                            \
                    }));                                                                                     \
        },                                                                                                   \
        &NKikimrClient::TGRpcServer::AsyncService::Y_CAT(Request, methodName),                               \
        "Legacy/" Y_STRINGIZE(methodName),                                                                   \
        logger,                                                                                              \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                                   \
    )->Run()                                                                                                 \
    /**/

    SETUP_SERVER_METHOD(SchemeOperation, TSchemeOperation, TResponse, DoSchemeOperation(MsgBusProxy, ActorSystem), Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(SchemeOperationStatus, TSchemeOperationStatus, TResponse, DoSchemeOperationStatus, Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(SchemeDescribe, TSchemeDescribe, TResponse, DoSchemeDescribe(MsgBusProxy, ActorSystem), Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(ChooseProxy, TChooseProxyRequest, TResponse, DoChooseProxy, Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(PersQueueRequest, TPersQueueRequest, TResponse, DoPersQueueRequest(MsgBusProxy, ActorSystem), Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_SERVER_METHOD(SchemeInitRoot, TSchemeInitRoot, TResponse, DoSchemeInitRoot(MsgBusProxy, ActorSystem), Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(ResolveNode, TResolveNodeRequest, TResponse, DoResolveNode, Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(FillNode, TFillNodeRequest, TResponse, DoFillNode, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(DrainNode, TDrainNodeRequest, TResponse, DoDrainNode, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(BlobStorageConfig, TBlobStorageConfigRequest, TResponse, DoBlobStorageConfig, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(HiveCreateTablet, THiveCreateTablet, TResponse, DoHiveCreateTablet, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(TestShardControl, TTestShardControlRequest, TResponse, DoTestShardControl, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(RegisterNode, TNodeRegistrationRequest, TNodeRegistrationResponse, DoRegisterNode, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::NodeRegistration));
    SETUP_SERVER_METHOD(CmsRequest, TCmsRequest, TCmsResponse, DoCmsRequest, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(ConsoleRequest, TConsoleRequest, TConsoleResponse, DoConsoleRequest, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(InterconnectDebug, TInterconnectDebug, TResponse, DoInterconnectDebug, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(TabletStateRequest, TTabletStateRequest, TResponse, DoTabletStateRequest, Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(LocalSchemeTx, TLocalSchemeTx, TResponse, DoLocalSchemeTx, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(LocalEnumerateTablets, TLocalEnumerateTablets, TResponse, DoLocalEnumerateTablets, Off, UNSPECIFIED, legacy, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(TabletKillRequest, TTabletKillRequest, TResponse, DoTabletKillRequest, Off, UNSPECIFIED, legacy, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
}

}
}
