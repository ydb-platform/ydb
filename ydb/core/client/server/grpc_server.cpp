#include "grpc_server.h"
#include "grpc_proxy_status.h"

#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/grpc/server/grpc_request.h>
#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/library/grpc/server/grpc_async_ctx_base.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/join.h>

#include <grpc++/resource_quota.h>
#include <grpc++/security/server_credentials.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/server.h>

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
namespace {

using TGrpcBaseAsyncContext = NYdbGrpc::TBaseAsyncContext<NGRpcProxy::TGRpcService>;

template <typename TIn, typename TOut = TResponse>
class TSimpleRequest
    : public IQueueEvent
    , public TGrpcBaseAsyncContext
    , public IRequestContext
{
    using TOnRequest = std::function<void (IRequestContext* ctx)>;

    using TRequestCallback = void (NKikimrClient::TGRpcServer::AsyncService::*)(ServerContext*, TIn*,
        ServerAsyncResponseWriter<TOut>*, CompletionQueue*, ServerCompletionQueue*, void*);

public:

    TSimpleRequest(TGRpcService* server,
                   NKikimrClient::TGRpcServer::AsyncService* service,
                   ServerCompletionQueue* cq,
                   TOnRequest cb,
                   TRequestCallback requestCallback,
                   TActorSystem& as,
                   const char* name,
                   NYdbGrpc::ICounterBlockPtr counters)
        : TGrpcBaseAsyncContext(service, cq)
        , Server(server)
        , Cb(cb)
        , RequestCallback(requestCallback)
        , ActorSystem(as)
        , Name(name)
        , Counters(std::move(counters))
        , Writer(new ServerAsyncResponseWriter<TOut>(&Context))
        , StateFunc(&TSimpleRequest::RequestDone)
        , RequestSize(0)
        , ResponseSize(0)
        , ResponseStatus(0)
        , InProgress_(false)
    {
        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] created request Name# %s", this, Name);
    }

    ~TSimpleRequest() {
        if (InProgress_) {
            //If we are ShuttingDown probably ActorSystem unable to recieve new events
            if (!Server->IsShuttingDown()) {
                LOG_ERROR(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] request destroyed with InProgress state Name# %s", this, Name);
            }
            Counters->FinishProcessing(RequestSize, ResponseSize, false, ResponseStatus,
                TDuration::Seconds(RequestTimer.Passed()));
            Server->DecRequest();
        }
    }

    void Start() {
        if (auto guard = Server->ProtectShutdown()) {
            OnBeforeCall();
            (Service->*RequestCallback)(&Context, &Request, Writer.Get(), CQ, CQ, GetGRpcTag());
        } else {
            // Server is shutting down, new requests cannot be started
            delete this;
        }
    }

public:
    //! Start another instance of request to grab next incoming query (only when the server is not shutting down)
    void Clone() {
        if (!Server->IsShuttingDown()) {
            if (RequestCallback)
                (new TSimpleRequest(Server, Service, CQ, Cb, RequestCallback, ActorSystem, Name, Counters))->Start();
        }
    }

    bool Execute(bool ok) override {
        return (this->*StateFunc)(ok);
    }

    void DestroyRequest() override {
        Y_ABORT_UNLESS(!CallInProgress_, "Unexpected DestroyRequest while another grpc call is still in progress");
        RequestDestroyed_ = true;
        if (RequestRegistered_) {
            Server->DeregisterRequestCtx(this);
            RequestRegistered_ = false;
        }
        delete this;
    }

private:
    void OnBeforeCall() {
        Y_ABORT_UNLESS(!RequestDestroyed_, "Cannot start grpc calls after request is already destroyed");
        Y_ABORT_UNLESS(!Finished_, "Cannot start grpc calls after request is finished");
        bool wasInProgress = std::exchange(CallInProgress_, true);
        Y_ABORT_UNLESS(!wasInProgress, "Another grpc call is already in progress");
    }

    void OnAfterCall() {
        Y_ABORT_UNLESS(!RequestDestroyed_, "Finished grpc call after request is already destroyed");
        bool wasInProgress = std::exchange(CallInProgress_, false);
        Y_ABORT_UNLESS(wasInProgress, "Finished grpc call that was not in progress");
    }

public:
    //! Get pointer to the request's message.
    const NProtoBuf::Message* GetRequest() const override {
        return &Request;
    }

    //! Send reply.
    void Reply(const NKikimrClient::TResponse& resp) override {
        if (const TOut* x = dynamic_cast<const TOut*>(&resp)) {
            Finish(*x, 0);
        } else {
            ReplyError(resp.GetErrorReason());
        }
    }

    void Reply(const NKikimrClient::TDsTestLoadResponse& resp) override {
        if (const TOut* x = dynamic_cast<const TOut*>(&resp)) {
            Finish(*x, 0);
        } else {
            ReplyError("request failed");
        }
    }

    void Reply(const NKikimrClient::TBsTestLoadResponse& resp) override {
        if (const TOut* x = dynamic_cast<const TOut*>(&resp)) {
            Finish(*x, 0);
        } else {
            ReplyError("request failed");
        }
    }

    void Reply(const NKikimrClient::TJSON& resp) override {
        try {
            Finish(dynamic_cast<const TOut&>(resp), 0);
        } catch (const std::bad_cast&) {
            Y_ABORT("unexpected response type generated");
        }
    }

    void Reply(const NKikimrClient::TNodeRegistrationResponse& resp) override {
        try {
            Finish(dynamic_cast<const TOut&>(resp), 0);
        } catch (const std::bad_cast&) {
            Y_ABORT("unexpected response type generated");
        }
    }

    void Reply(const NKikimrClient::TCmsResponse& resp) override {
        try {
            Finish(dynamic_cast<const TOut&>(resp), 0);
        } catch (const std::bad_cast&) {
            Y_ABORT("unexpected response type generated");
        }
    }

    void Reply(const NKikimrClient::TSqsResponse& resp) override {
        try {
            Finish(dynamic_cast<const TOut&>(resp), 0);
        } catch (const std::bad_cast&) {
            Y_ABORT("unexpected response type generated");
        }
    }

    void Reply(const NKikimrClient::TConsoleResponse& resp) override {
        try {
            Finish(dynamic_cast<const TOut&>(resp), 0);
        } catch (const std::bad_cast&) {
            Y_ABORT("unexpected response type generated");
        }
    }

    //! Send error reply.
    void ReplyError(const TString& reason) override {
        TOut resp;
        GenerateErrorResponse(resp, reason);
        Finish(resp, 0);
    }

    static void GenerateErrorResponse(NKikimrClient::TResponse& resp, const TString& reason) {
        resp.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        if (reason) {
            resp.SetErrorReason(reason);
        }
    }

    static void GenerateErrorResponse(NKikimrClient::TNodeRegistrationResponse& resp, const TString& reason) {
        resp.MutableStatus()->SetCode(NKikimrNodeBroker::TStatus::ERROR);
        resp.MutableStatus()->SetReason(reason);
    }

    static void GenerateErrorResponse(NKikimrClient::TCmsResponse& resp, const TString& reason) {
        resp.MutableStatus()->SetCode(NKikimrCms::TStatus::ERROR);
        resp.MutableStatus()->SetReason(reason);
    }

    static void GenerateErrorResponse(NKikimrClient::TJSON& resp, const TString& reason) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["ErrorReason"] = reason;
        resp.SetJSON(NJson::WriteJson(json, false));
    }

    static void GenerateErrorResponse(NKikimrClient::TSqsResponse&, const TString&)
    { }

    static void GenerateErrorResponse(NKikimrClient::TConsoleResponse& resp, const TString& reason) {
        resp.MutableStatus()->SetCode(Ydb::StatusIds::GENERIC_ERROR);
        resp.MutableStatus()->SetReason(reason);
    }

    NMsgBusProxy::TBusMessageContext BindBusContext(int type) override {
        return BusContext.ConstructInPlace(this, type);
    }

    TString GetPeer() const override {
        return GetPeerName();
    }

    TVector<TStringBuf> FindClientCert() const override {
        return TGrpcBaseAsyncContext::FindClientCert();
    }

private:
    void* GetGRpcTag() {
        return static_cast<IQueueEvent*>(this);
    }

    void Finish(const TOut& resp, ui32 status) {
        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] issuing response Name# %s data# %s peer# %s", this,
            Name, NYdbGrpc::FormatMessage<TOut>(resp).data(), GetPeerName().c_str());
        ResponseSize = resp.ByteSize();
        ResponseStatus = status;
        StateFunc = &TSimpleRequest::FinishDone;
        OnBeforeCall();
        Finished_ = true;
        Writer->Finish(resp, Status::OK, GetGRpcTag());
    }

    void FinishNoResource() {
        TOut resp;
        TString msg = "no resource";
        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] issuing response Name# %s nodata (no resources) peer# %s", this,
            Name, GetPeerName().c_str());

        StateFunc = &TSimpleRequest::FinishDoneWithoutProcessing;
        OnBeforeCall();
        Finished_ = true;
        Writer->Finish(resp,
                       grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, msg),
                       GetGRpcTag());
    }

    bool RequestDone(bool ok) {
        OnAfterCall();

        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] received request Name# %s ok# %s data# %s peer# %s current inflight# %li", this,
            Name, ok ? "true" : "false", NYdbGrpc::FormatMessage<TIn>(Request, ok).data(), GetPeerName().c_str(), Server->GetCurrentInFlight());

        if (Context.c_call() == nullptr) {
            Y_ABORT_UNLESS(!ok);
        } else if (!(RequestRegistered_ = Server->RegisterRequestCtx(this))) {
            // Request cannot be registered due to shutdown
            // It's unsafe to continue, so drop this request without processing
            LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] dropped request Name# %s due to shutdown",
                this, Name);
            Context.TryCancel();
            return false;
        }

        Clone();

        if (!ok) {
            Counters->CountNotOkRequest();
            return false;
        }

        if (Server->IncRequest()) {

            RequestSize = Request.ByteSize();
            Counters->StartProcessing(RequestSize);
            RequestTimer.Reset();
            InProgress_ = true;

            Cb(this);
        } else {
            FinishNoResource();
        }

        return true;
    }

    bool FinishDone(bool ok) {
        OnAfterCall();
        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] finished request Name# %s ok# %s peer# %s", this,
            Name, ok ? "true" : "false", GetPeerName().c_str());
        Counters->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
            TDuration::Seconds(RequestTimer.Passed()));
        Server->DecRequest();
        InProgress_ = false;

        return false;
    }

    bool FinishDoneWithoutProcessing(bool ok) {
        OnAfterCall();
        LOG_DEBUG(ActorSystem, NKikimrServices::GRPC_SERVER, "[%p] finished request without processing Name# %s ok# %s peer# %s", this,
            Name, ok ? "true" : "false", GetPeerName().c_str());

        return false;
    }

private:
    using TStateFunc = bool (TSimpleRequest::*)(bool);

    TGRpcService* const Server;
    TOnRequest Cb;
    TRequestCallback RequestCallback;
    TActorSystem& ActorSystem;
    const char* const Name;
    NYdbGrpc::ICounterBlockPtr Counters;

    THolder<ServerAsyncResponseWriter<TOut>> Writer;

    TStateFunc StateFunc;
    TIn Request;
    ui32 RequestSize;
    ui32 ResponseSize;
    ui32 ResponseStatus;
    THPTimer RequestTimer;

    TMaybe<NMsgBusProxy::TBusMessageContext> BusContext;
    bool InProgress_;
    bool RequestRegistered_ = false;
    bool RequestDestroyed_ = false;
    bool CallInProgress_ = false;
    bool Finished_ = false;
};

} // namespace

TGRpcService::TGRpcService()
    : ActorSystem(nullptr)
{}

void TGRpcService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr) {
    CQ = cq;
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
    SetupIncomingRequests();
}

void TGRpcService::RegisterRequestActor(NActors::IActor* req) {
    ActorSystem->Register(req, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
}

void TGRpcService::SetupIncomingRequests() {

    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    (new TSimpleRequest<NKikimrClient::IN, NKikimrClient::OUT>(this, &Service_, CQ, \
        [this](IRequestContext *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, ctx->GetPeer()); \
            ACTION; \
        }, &NKikimrClient::TGRpcServer::AsyncService::Request ## NAME, \
        *ActorSystem, #NAME, getCounterBlock("legacy", #NAME)))->Start();

#define ADD_ACTOR_REQUEST(NAME, TYPE, MTYPE) \
    ADD_REQUEST(NAME, TYPE, TResponse, { \
        NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE)); \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem, ctx->GetPeer()); \
        RegisterRequestActor(CreateMessageBus ## NAME(msg)); \
    })


    // actor requests
    ADD_ACTOR_REQUEST(BSAdm,                     TBSAdm,                            MTYPE_CLIENT_BSADM)
    ADD_ACTOR_REQUEST(BlobStorageConfig,         TBlobStorageConfigRequest,         MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST)
    ADD_ACTOR_REQUEST(HiveCreateTablet,          THiveCreateTablet,                 MTYPE_CLIENT_HIVE_CREATE_TABLET)
    ADD_ACTOR_REQUEST(LocalEnumerateTablets,     TLocalEnumerateTablets,            MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS)
    ADD_ACTOR_REQUEST(KeyValue,                  TKeyValueRequest,                  MTYPE_CLIENT_KEYVALUE)
    ADD_ACTOR_REQUEST(TabletStateRequest,        TTabletStateRequest,               MTYPE_CLIENT_TABLET_STATE_REQUEST)
    ADD_ACTOR_REQUEST(LocalMKQL,                 TLocalMKQL,                        MTYPE_CLIENT_LOCAL_MINIKQL)
    ADD_ACTOR_REQUEST(LocalSchemeTx,             TLocalSchemeTx,                    MTYPE_CLIENT_LOCAL_SCHEME_TX)
    ADD_ACTOR_REQUEST(TabletKillRequest,         TTabletKillRequest,                MTYPE_CLIENT_TABLET_KILL_REQUEST)
    ADD_ACTOR_REQUEST(SchemeOperationStatus,     TSchemeOperationStatus,            MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST)
    ADD_ACTOR_REQUEST(BlobStorageLoadRequest,    TBsTestLoadRequest,                MTYPE_CLIENT_LOAD_REQUEST)
    ADD_ACTOR_REQUEST(BlobStorageGetRequest,     TBsGetRequest,                     MTYPE_CLIENT_GET_REQUEST)
    ADD_ACTOR_REQUEST(ChooseProxy,               TChooseProxyRequest,               MTYPE_CLIENT_CHOOSE_PROXY)
    ADD_ACTOR_REQUEST(WhoAmI,                    TWhoAmI,                           MTYPE_CLIENT_WHOAMI)
    ADD_ACTOR_REQUEST(ResolveNode,               TResolveNodeRequest,               MTYPE_CLIENT_RESOLVE_NODE)
    ADD_ACTOR_REQUEST(FillNode,                  TFillNodeRequest,                  MTYPE_CLIENT_FILL_NODE)
    ADD_ACTOR_REQUEST(DrainNode,                 TDrainNodeRequest,                 MTYPE_CLIENT_DRAIN_NODE)
    ADD_ACTOR_REQUEST(InterconnectDebug,         TInterconnectDebug,                MTYPE_CLIENT_INTERCONNECT_DEBUG)
    ADD_ACTOR_REQUEST(TestShardControl,          TTestShardControlRequest,          MTYPE_CLIENT_TEST_SHARD_CONTROL)
    ADD_ACTOR_REQUEST(LoginRequest,              TLoginRequest,                     MTYPE_CLIENT_LOGIN_REQUEST)

    // dynamic node registration
    ADD_REQUEST(RegisterNode, TNodeRegistrationRequest, TNodeRegistrationResponse, {
        NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE_CLIENT_NODE_REGISTRATION_REQUEST));
        RegisterRequestActor(CreateMessageBusRegisterNode(msg));
    })

    // CMS request
    ADD_REQUEST(CmsRequest, TCmsRequest, TCmsResponse, {
        NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE_CLIENT_CMS_REQUEST));
        RegisterRequestActor(CreateMessageBusCmsRequest(msg));
    })

    // SQS request
    ADD_REQUEST(SqsRequest, TSqsRequest, TSqsResponse, {
        NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE_CLIENT_SQS_REQUEST));
        RegisterRequestActor(CreateMessageBusSqsRequest(msg));
    })

    // Console request
    ADD_REQUEST(ConsoleRequest, TConsoleRequest, TConsoleResponse, {
        NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE_CLIENT_CONSOLE_REQUEST));
        RegisterRequestActor(CreateMessageBusConsoleRequest(msg));
    })

#define ADD_PROXY_REQUEST_BASE(NAME, TYPE, RES_TYPE, EVENT_TYPE, MTYPE) \
    ADD_REQUEST(NAME, TYPE, RES_TYPE, { \
        if (MsgBusProxy) { \
            NMsgBusProxy::TBusMessageContext msg(ctx->BindBusContext(NMsgBusProxy::MTYPE)); \
            ActorSystem->Send(MsgBusProxy, new NMsgBusProxy::EVENT_TYPE(msg)); \
        } else { \
            ctx->ReplyError("no MessageBus proxy"); \
        } \
    })

#define ADD_PROXY_REQUEST(NAME, TYPE, EVENT_TYPE, MTYPE) \
    ADD_PROXY_REQUEST_BASE(NAME, TYPE, TResponse, EVENT_TYPE, MTYPE)

    // proxy requests
    ADD_PROXY_REQUEST(SchemeInitRoot,  TSchemeInitRoot,  TEvBusProxy::TEvInitRoot,            MTYPE_CLIENT_SCHEME_INITROOT)

    ADD_PROXY_REQUEST(PersQueueRequest, TPersQueueRequest, TEvBusProxy::TEvPersQueue,           MTYPE_CLIENT_PERSQUEUE)
    ADD_PROXY_REQUEST(Request,          TRequest,          TEvBusProxy::TEvRequest,             MTYPE_CLIENT_REQUEST)
    ADD_PROXY_REQUEST(SchemeOperation,  TSchemeOperation,  TEvBusProxy::TEvFlatTxRequest,       MTYPE_CLIENT_FLAT_TX_REQUEST)
    ADD_PROXY_REQUEST(SchemeDescribe,   TSchemeDescribe,   TEvBusProxy::TEvFlatDescribeRequest, MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST)

#define ADD_PROXY_REQUEST_JJ(NAME, EVENT_TYPE, MTYPE) \
    ADD_PROXY_REQUEST_BASE(NAME, TJSON, TJSON, EVENT_TYPE, MTYPE)

    // DB proxy requests both consuming and returning TJSON
    ADD_PROXY_REQUEST_JJ(DbSchema,    TEvBusProxy::TEvDbSchema,    MTYPE_CLIENT_DB_SCHEMA)
    ADD_PROXY_REQUEST_JJ(DbOperation, TEvBusProxy::TEvDbOperation, MTYPE_CLIENT_DB_OPERATION)
    ADD_PROXY_REQUEST_JJ(DbBatch,     TEvBusProxy::TEvDbBatch,     MTYPE_CLIENT_DB_BATCH)
}

}
}
