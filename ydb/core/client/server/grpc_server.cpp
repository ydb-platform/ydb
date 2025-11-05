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
#include <ydb/library/grpc/server/grpc_method_setup.h>
#include <ydb/core/protos/node_broker.pb.h>

#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_base.pb.h>

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
    : ActorSystem_(nullptr)
    , GRpcRequestProxyId_(grpcRequestProxyId)
{}

void TGRpcService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    Logger_ = std::move(logger);
    Y_ASSERT(InitCb_);
    InitCb_();
}

TFuture<void> TGRpcService::Prepare(TActorSystem* system, const TActorId& pqMeta, const TActorId& msgBusProxy,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    auto promise = NewPromise<void>();
    InitCb_ = [=, this]() mutable {
        try {
            ActorSystem_ = system;
            PQMeta_ = pqMeta;
            MsgBusProxy_ = msgBusProxy;
            Counters_ = counters;

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
    Y_ABORT_UNLESS(ActorSystem_);
    ui32 nodeId = ActorSystem_->NodeId;
    ActorSystem_->Send(MakeGRpcProxyStatusID(nodeId), new TEvGRpcProxyStatus::TEvSetup(true, PersQueueWriteSessionsMaxCount_,
                                        PersQueueReadSessionsMaxCount_));
    SetupIncomingRequests(Logger_);
}

void TGRpcService::RegisterRequestActor(NActors::IActor* req) {
    ActorSystem_->Register(req, TMailboxType::HTSwap, ActorSystem_->AppData<TAppData>()->UserPoolId);
}

template <typename TReq, typename TResp, NKikimr::NGRpcService::NRuntimeEvents::EType RuntimeEventType = NKikimr::NGRpcService::NRuntimeEvents::EType::COMMON>
using TGrpcRequestLegacyCall = NKikimr::NGRpcService::TGrpcRequestNoOperationCall<TReq, TResp, RuntimeEventType, NKikimr::NGRpcService::NLegacyGrpcService::TLegacyGrpcMethodAccessorTraits<TReq, TResp>>;

void TGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace ::NKikimr::NGRpcService;
    using namespace ::NKikimr::NGRpcService::NLegacyGrpcService;
    using namespace NKikimrClient;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_SERVER_METHOD
#error SETUP_SERVER_METHOD macro already defined
#endif

#define SETUP_SERVER_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                 \
        inputType,                                         \
        outputType,                                        \
        methodCallback,                                    \
        rlMode,                                            \
        requestType,                                       \
        YDB_API_DEFAULT_COUNTER_BLOCK(legacy, methodName), \
        auditMode,                                         \
        COMMON,                                            \
        TGrpcRequestLegacyCall,                            \
        GRpcRequestProxyId_,                               \
        CQ_,                                               \
        nullptr,                                           \
        nullptr)

    SETUP_SERVER_METHOD(SchemeOperation, TSchemeOperation, TResponse, DoSchemeOperation(MsgBusProxy_, ActorSystem_), RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(SchemeOperationStatus, TSchemeOperationStatus, TResponse, DoSchemeOperationStatus, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(SchemeDescribe, TSchemeDescribe, TResponse, DoSchemeDescribe(MsgBusProxy_, ActorSystem_), RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(ChooseProxy, TChooseProxyRequest, TResponse, DoChooseProxy, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(PersQueueRequest, TPersQueueRequest, TResponse, DoPersQueueRequest(MsgBusProxy_, ActorSystem_), RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_SERVER_METHOD(SchemeInitRoot, TSchemeInitRoot, TResponse, DoSchemeInitRoot(MsgBusProxy_, ActorSystem_), RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(ResolveNode, TResolveNodeRequest, TResponse, DoResolveNode, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_SERVER_METHOD(FillNode, TFillNodeRequest, TResponse, DoFillNode, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(DrainNode, TDrainNodeRequest, TResponse, DoDrainNode, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(BlobStorageConfig, TBlobStorageConfigRequest, TResponse, DoBlobStorageConfig, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(HiveCreateTablet, THiveCreateTablet, TResponse, DoHiveCreateTablet, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(TestShardControl, TTestShardControlRequest, TResponse, DoTestShardControl, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(RegisterNode, TNodeRegistrationRequest, TNodeRegistrationResponse, DoRegisterNode, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::NodeRegistration));
    SETUP_SERVER_METHOD(CmsRequest, TCmsRequest, TCmsResponse, DoCmsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(ConsoleRequest, TConsoleRequest, TConsoleResponse, DoConsoleRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(InterconnectDebug, TInterconnectDebug, TResponse, DoInterconnectDebug, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_SERVER_METHOD(TabletStateRequest, TTabletStateRequest, TResponse, DoTabletStateRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_SERVER_METHOD
}

}
}
