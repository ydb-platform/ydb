#include <library/cpp/monlib/messagebus/mon_messagebus.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "msgbus_server.h"
#include "msgbus_http_server.h"
#include "grpc_server.h"

namespace NKikimr {
namespace NMsgBusProxy {

class TBusMessageContext::TImpl : public TThrRefBase {
public:
    virtual ~TImpl() = default;
    virtual NBus::TBusMessage* GetMessage() = 0;
    virtual NBus::TBusMessage* ReleaseMessage() = 0;
    virtual void SendReplyMove(NBus::TBusMessageAutoPtr response) = 0;
    virtual TVector<TStringBuf> FindClientCert() const = 0;
    virtual THolder<TMessageBusSessionIdentHolder::TImpl> CreateSessionIdentHolder() = 0;
};

class TBusMessageContext::TImplMessageBus
    : public TBusMessageContext::TImpl
    , private NBus::TOnMessageContext
    , private TBusMessageWatcher
{
public:
    TImplMessageBus()
    {}

    TImplMessageBus(NBus::TOnMessageContext &messageContext, IMessageWatcher *messageWatcher)
        : TBusMessageWatcher(messageWatcher)
    {
        NBus::TOnMessageContext::Swap(messageContext);
    }

    ~TImplMessageBus() {
        if (NBus::TOnMessageContext::IsInWork()) {
            NotifyForget();
            NBus::TOnMessageContext::ForgetRequest();
        }
    }

    NBus::TBusMessage* GetMessage() override {
        return NBus::TOnMessageContext::GetMessage();
    }

    NBus::TBusMessage* ReleaseMessage() override {
        return NBus::TOnMessageContext::ReleaseMessage();
    }

    void SendReplyMove(NBus::TBusMessageAutoPtr response) override {
        NotifyReply(response.Get());
        NBus::TOnMessageContext::SendReplyMove(response);
    }

    NBus::TBusKey GetMessageId() override {
        return GetMessage()->GetHeader()->Id;
    }

    TVector<TStringBuf> FindClientCert() const override {
        return {};
    }


    THolder<TMessageBusSessionIdentHolder::TImpl> CreateSessionIdentHolder() override;
};

class TBusMessageContext::TImplGRpc
    : public TBusMessageContext::TImpl
{
    NGRpcProxy::IRequestContext *RequestContext;
    THolder<NBus::TBusMessage> Message;

public:
    TImplGRpc(NGRpcProxy::IRequestContext *requestContext, int type)
        : RequestContext(requestContext)
    {
        switch (type) {
#define MTYPE(TYPE) \
            case TYPE::MessageType: \
                Message.Reset(new TYPE()); \
                try { \
                    static_cast<TYPE&>(*Message).Record = dynamic_cast<const TYPE::RecordType&>(*RequestContext->GetRequest()); \
                } catch (const std::bad_cast&) { \
                    Y_ABORT("incorrect request message type"); \
                } \
                return;

            MTYPE(TBusRequest)
            MTYPE(TBusResponse)
            MTYPE(TBusFakeConfigDummy)
            MTYPE(TBusSchemeInitRoot)
            MTYPE(TBusTypesRequest)
            MTYPE(TBusTypesResponse)
            MTYPE(TBusHiveCreateTablet)
            MTYPE(TBusOldHiveCreateTablet)
            MTYPE(TBusHiveCreateTabletResult)
            MTYPE(TBusLocalEnumerateTablets)
            MTYPE(TBusOldLocalEnumerateTablets)
            MTYPE(TBusLocalEnumerateTabletsResult)
            MTYPE(TBusKeyValue)
            MTYPE(TBusOldKeyValue)
            MTYPE(TBusKeyValueResponse)
            MTYPE(TBusPersQueue)
            MTYPE(TBusTabletKillRequest)
            MTYPE(TBusTabletStateRequest)
            MTYPE(TBusTabletCountersRequest)
            MTYPE(TBusTabletLocalMKQL)
            MTYPE(TBusTabletLocalSchemeTx)
            MTYPE(TBusSchemeOperation)
            MTYPE(TBusSchemeOperationStatus)
            MTYPE(TBusSchemeDescribe)
            MTYPE(TBusOldFlatDescribeRequest)
            MTYPE(TBusOldFlatDescribeResponse)
            MTYPE(TBusBlobStorageConfigRequest)
            MTYPE(TBusNodeRegistrationRequest)
            MTYPE(TBusCmsRequest)
            MTYPE(TBusChooseProxy)
            MTYPE(TBusSqsRequest)
            MTYPE(TBusStreamRequest)
            MTYPE(TBusInterconnectDebug)
            MTYPE(TBusConsoleRequest)
            MTYPE(TBusResolveNode)
            MTYPE(TBusFillNode)
            MTYPE(TBusDrainNode)
            MTYPE(TBusTestShardControlRequest)
#undef MTYPE
        }

        Y_ABORT();
    }

    ~TImplGRpc() {
        ForgetRequest();
    }

    void ForgetRequest() {
        if (RequestContext) {
            RequestContext->ReplyError("request wasn't processed properly");
            RequestContext = nullptr;
        }
    }

    NBus::TBusMessage* GetMessage() override {
        return Message.Get();
    }

    NBus::TBusMessage* ReleaseMessage() override {
        return Message.Release();
    }

    void SendReply(NBus::TBusMessage *resp) {
        Y_ABORT_UNLESS(RequestContext);
        switch (const ui32 type = resp->GetHeader()->Type) {
#define REPLY_OPTION(TYPE) \
            case TYPE::MessageType: { \
                auto *msg = dynamic_cast<TYPE *>(resp); \
                Y_ABORT_UNLESS(msg); \
                RequestContext->Reply(msg->Record); \
                break; \
            }

            REPLY_OPTION(TBusResponse)
            REPLY_OPTION(TBusNodeRegistrationResponse)
            REPLY_OPTION(TBusCmsResponse)
            REPLY_OPTION(TBusSqsResponse)
            REPLY_OPTION(TBusConsoleResponse)

            default:
                Y_ABORT("unexpected response type %" PRIu32, type);
        }
        RequestContext = nullptr;
    }

    void SendReplyMove(NBus::TBusMessageAutoPtr response) override {
        SendReply(response.Get());
    }

    TVector<TStringBuf> FindClientCert() const override {
        return RequestContext->FindClientCert();
    };

    THolder<TMessageBusSessionIdentHolder::TImpl> CreateSessionIdentHolder() override;
};

TBusMessageContext::TBusMessageContext()
{}

TBusMessageContext::TBusMessageContext(const TBusMessageContext& other)
    : Impl(other.Impl)
{}

TBusMessageContext::TBusMessageContext(NBus::TOnMessageContext &messageContext, IMessageWatcher *messageWatcher)
    : Impl(new TImplMessageBus(messageContext, messageWatcher))
{}

TBusMessageContext::TBusMessageContext(NGRpcProxy::IRequestContext *requestContext, int type)
    : Impl(new TImplGRpc(requestContext, type))
{}

TBusMessageContext::~TBusMessageContext()
{}

TBusMessageContext& TBusMessageContext::operator =(TBusMessageContext other) {
    Impl = std::move(other.Impl);
    return *this;
}

NBus::TBusMessage *TBusMessageContext::GetMessage() {
    Y_ABORT_UNLESS(Impl);
    return Impl->GetMessage();
}

NBus::TBusMessage *TBusMessageContext::ReleaseMessage() {
    Y_ABORT_UNLESS(Impl);
    return Impl->ReleaseMessage();
}

void TBusMessageContext::SendReplyMove(NBus::TBusMessageAutoPtr response) {
    Y_ABORT_UNLESS(Impl);
    Impl->SendReplyMove(response);
}

void TBusMessageContext::Swap(TBusMessageContext &msg) {
    std::swap(Impl, msg.Impl);
}

TVector<TStringBuf> TBusMessageContext::FindClientCert() const { return Impl->FindClientCert(); }

THolder<TMessageBusSessionIdentHolder::TImpl> TBusMessageContext::CreateSessionIdentHolder() {
    Y_ABORT_UNLESS(Impl);
    return Impl->CreateSessionIdentHolder();
}


class TMessageBusSessionIdentHolder::TImpl {
public:
    virtual ~TImpl() = default;
    virtual void SendReply(NBus::TBusMessage *resp) = 0;
    virtual void SendReplyMove(NBus::TBusMessageAutoPtr resp) = 0;
    virtual ui64 GetTotalTimeout() const = 0;
    virtual TVector<TStringBuf> FindClientCert() const = 0;

};

class TMessageBusSessionIdentHolder::TImplMessageBus
    : public TMessageBusSessionIdentHolder::TImpl
    , private TBusMessageWatcher
{
    NBus::TBusServerSession *Session;
    NBus::TBusIdentity Ident;

public:
    TImplMessageBus(NBus::TOnMessageContext &ctx) {
        Session = ctx.GetSession();
        ctx.AckMessage(Ident);
    }

    ~TImplMessageBus() {
        if (Session && Ident.IsInWork()) {
            NotifyForget();
            Session->ForgetRequest(Ident);
        }
    }

    void SendReply(NBus::TBusMessage *resp) override {
        NotifyReply(resp);
        Session->SendReply(Ident, resp);
    }

    void SendReplyMove(NBus::TBusMessageAutoPtr resp) override {
        NotifyReply(resp.Get());
        Session->SendReplyMove(Ident, resp);
    }

    NBus::TBusKey GetMessageId() override {
        return Ident.MessageId;
    }

    ui64 GetTotalTimeout() const override {
        return Session->GetConfig()->TotalTimeout;
    }

    TVector<TStringBuf> FindClientCert() const override {
        return {};
    }
};

THolder<TMessageBusSessionIdentHolder::TImpl> TBusMessageContext::TImplMessageBus::CreateSessionIdentHolder() {
    return MakeHolder<TMessageBusSessionIdentHolder::TImplMessageBus>(static_cast<NBus::TOnMessageContext&>(*this));
}

class TMessageBusSessionIdentHolder::TImplGRpc
    : public TMessageBusSessionIdentHolder::TImpl
{
    TIntrusivePtr<TBusMessageContext::TImplGRpc> Context;

public:
    TImplGRpc(TIntrusivePtr<TBusMessageContext::TImplGRpc> context)
        : Context(context)
    {
    }

    ~TImplGRpc() {
        if (Context) {
            Context->ForgetRequest();
        }
    }

    void SendReply(NBus::TBusMessage *resp) override {
        Y_ABORT_UNLESS(Context);
        Context->SendReply(resp);

        auto context = std::move(Context);
    }

    void SendReplyMove(NBus::TBusMessageAutoPtr resp) override {
        Y_ABORT_UNLESS(Context);
        Context->SendReplyMove(resp);

        auto context = std::move(Context);
    }

    TVector<TStringBuf> FindClientCert() const override {
        return Context->FindClientCert();
    }

    ui64 GetTotalTimeout() const override {
        return 90000;
    }
};

THolder<TMessageBusSessionIdentHolder::TImpl> TBusMessageContext::TImplGRpc::CreateSessionIdentHolder() {
    return MakeHolder<TMessageBusSessionIdentHolder::TImplGRpc>(this);
}

TMessageBusSessionIdentHolder::TMessageBusSessionIdentHolder()
{}

TMessageBusSessionIdentHolder::TMessageBusSessionIdentHolder(TBusMessageContext &msg)
{
    InitSession(msg);
}

TMessageBusSessionIdentHolder::~TMessageBusSessionIdentHolder()
{}

void TMessageBusSessionIdentHolder::InitSession(TBusMessageContext &msg) {
    Impl = msg.CreateSessionIdentHolder();
}

ui64 TMessageBusSessionIdentHolder::GetTotalTimeout() const {
    Y_ABORT_UNLESS(Impl);
    return Impl->GetTotalTimeout();
}

void TMessageBusSessionIdentHolder::SendReply(NBus::TBusMessage *resp) {
    Y_ABORT_UNLESS(Impl);
    Impl->SendReply(resp);
}

void TMessageBusSessionIdentHolder::SendReplyMove(NBus::TBusMessageAutoPtr resp) {
    Y_ABORT_UNLESS(Impl);
    Impl->SendReplyMove(resp);
}

TVector<TStringBuf> TMessageBusSessionIdentHolder::FindClientCert() const {
    return Impl->FindClientCert();
}


void TBusMessageWatcher::NotifyForget() {
    if (MessageWatcher) {
        MessageWatcher->OnMessageDied(GetMessageId());
        MessageWatcher = nullptr;
    }
}

void TBusMessageWatcher::NotifyReply(NBus::TBusMessage *response) {
    if (MessageWatcher) {
        MessageWatcher->OnMessageReplied(GetMessageId(), response);
        MessageWatcher = nullptr;
    }
}



class TMessageBusMonitorActor : public TActorBootstrapped<TMessageBusMonitorActor> {
    struct TEvPrivate {
        enum EEv {
            EvUpdateMsgBusStats = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvUpdateMsgBusStats : TEventLocal<TEvUpdateMsgBusStats, EvUpdateMsgBusStats> {};
    };

    constexpr static TDuration::TValue Interval = TDuration::Seconds(10).GetValue();

public:
    TMessageBusMonitorActor(NBus::TBusServerSessionPtr session, const NBus::TBusServerSessionConfig &sessionConfig)
        : Session(session)
        , SessionConfig(sessionConfig)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MSGBUS_COMMON; }

    void Bootstrap(const TActorContext &ctx) {
        WhiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId());
        ctx.Send(WhiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddEndpoint("mbus", Sprintf(":%d", Session->GetProto()->GetPort())));
        Become(&TMessageBusMonitorActor::StateWork, ctx, TDuration::MicroSeconds(Interval), new TEvPrivate::TEvUpdateMsgBusStats());
    }

protected:
    void Handle(TEvPrivate::TEvUpdateMsgBusStats::TPtr, const TActorContext &ctx) {
        NKikimrWhiteboard::TSystemStateInfo systemStateInfo;
        auto inFlightPercent = SessionConfig.MaxInFlight > 0 ? Session->GetInFlight() * 100 / SessionConfig.MaxInFlight : 0;
        if (inFlightPercent < 75) {
            systemStateInfo.SetMessageBusState(NKikimrWhiteboard::EFlag::Green);
        } else
        if (inFlightPercent < 85) {
            systemStateInfo.SetMessageBusState(NKikimrWhiteboard::EFlag::Yellow);
        } else
        if (inFlightPercent < 95) {
            systemStateInfo.SetMessageBusState(NKikimrWhiteboard::EFlag::Orange);
        } else {
            systemStateInfo.SetMessageBusState(NKikimrWhiteboard::EFlag::Red);
        }
        ctx.Send(WhiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(systemStateInfo));
        ctx.Schedule(TDuration::MicroSeconds(Interval), new TEvPrivate::TEvUpdateMsgBusStats());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvUpdateMsgBusStats, Handle);
        }
    }

    TActorId WhiteboardServiceId;
    NBus::TBusServerSessionPtr Session;
    const NBus::TBusServerSessionConfig &SessionConfig;
};

TMessageBusServer::TMessageBusServer(
    const NBus::TBusServerSessionConfig &sessionConfig,
    NBus::TBusMessageQueue *busQueue,
    ui32 bindPort
)
    : SessionConfig(sessionConfig)
    , BusQueue(busQueue)
    , Protocol(bindPort)
{}

TMessageBusServer::~TMessageBusServer() {

}

void TMessageBusServer::InitSession(TActorSystem *actorSystem, const TActorId &proxy) {
    ActorSystem = actorSystem;
    Proxy = proxy;
    Session = NBus::TBusServerSession::Create(&Protocol, this, SessionConfig, BusQueue);
    HttpServer.Reset(CreateMessageBusHttpServer(actorSystem, this, Protocol, SessionConfig));
    Monitor = ActorSystem->Register(new TMessageBusMonitorActor(Session, SessionConfig), TMailboxType::HTSwap,
        actorSystem->AppData<TAppData>()->UserPoolId);
}

void TMessageBusServer::ShutdownSession() {
    HttpServer.Reset();
    if (Session) {
        Session->Shutdown();
    }
}

void TMessageBusServer::RegisterMonPage(NMonitoring::TBusNgMonPage *busMonPage) {
    busMonPage->BusWww->RegisterServerSession(Session);
}

void TMessageBusServer::OnMessage(NBus::TOnMessageContext &msg) {
    TBusMessageContext messageContext(msg);
    OnMessage(messageContext);
}

void TMessageBusServer::OnMessage(TBusMessageContext &msg) {
    const ui32 msgType = msg.GetMessage()->GetHeader()->Type;

    switch (msgType) {
    case MTYPE_CLIENT_REQUEST:
        return ClientProxyRequest<TEvBusProxy::TEvRequest>(msg);
    case MTYPE_CLIENT_SCHEME_INITROOT:
        return ClientProxyRequest<TEvBusProxy::TEvInitRoot>(msg);
    case MTYPE_CLIENT_SCHEME_NAVIGATE:
        return ClientProxyRequest<TEvBusProxy::TEvNavigate>(msg);
    case MTYPE_CLIENT_TYPES_REQUEST:
        return GetTypes(msg);
    case MTYPE_CLIENT_HIVE_CREATE_TABLET:
    case MTYPE_CLIENT_OLD_HIVE_CREATE_TABLET:
        return ClientActorRequest(CreateMessageBusHiveCreateTablet, msg);
    case MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS:
    case MTYPE_CLIENT_OLD_LOCAL_ENUMERATE_TABLETS:
        return ClientActorRequest(CreateMessageBusLocalEnumerateTablets, msg);
    case MTYPE_CLIENT_KEYVALUE:
    case MTYPE_CLIENT_OLD_KEYVALUE:
        return ClientActorRequest(CreateMessageBusKeyValue, msg);
    case MTYPE_CLIENT_PERSQUEUE:
        return ClientProxyRequest<TEvBusProxy::TEvPersQueue>(msg);
    case MTYPE_CLIENT_CHOOSE_PROXY:
        return ClientActorRequest(CreateMessageBusChooseProxy, msg);
    case MTYPE_CLIENT_TABLET_STATE_REQUEST:
        return ClientActorRequest(CreateMessageBusTabletStateRequest, msg);
    case MTYPE_CLIENT_TABLET_COUNTERS_REQUEST:
        return ClientActorRequest(CreateMessageBusTabletCountersRequest, msg);
    case MTYPE_CLIENT_LOCAL_MINIKQL:
        return ClientActorRequest(CreateMessageBusLocalMKQL, msg);
    case MTYPE_CLIENT_LOCAL_SCHEME_TX:
        return ClientActorRequest(CreateMessageBusLocalSchemeTx, msg);
    case MTYPE_CLIENT_TABLET_KILL_REQUEST:
        return ClientActorRequest(CreateMessageBusTabletKillRequest, msg);
    case MTYPE_CLIENT_FLAT_TX_REQUEST:
        return ClientProxyRequest<TEvBusProxy::TEvFlatTxRequest>(msg);
    case MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST:
        return ClientActorRequest(CreateMessageBusSchemeOperationStatus, msg);
    case MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST:
    case MTYPE_CLIENT_OLD_FLAT_DESCRIBE_REQUEST:
        return ClientProxyRequest<TEvBusProxy::TEvFlatDescribeRequest>(msg);
    case MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST:
        return ClientActorRequest(CreateMessageBusBlobStorageConfig, msg);
    case MTYPE_CLIENT_DRAIN_NODE:
        return ClientActorRequest(CreateMessageBusDrainNode, msg);
    case MTYPE_CLIENT_FILL_NODE:
        return ClientActorRequest(CreateMessageBusFillNode, msg);
    case MTYPE_CLIENT_RESOLVE_NODE:
        return ClientActorRequest(CreateMessageBusResolveNode, msg);
    case MTYPE_CLIENT_CMS_REQUEST:
        return ClientActorRequest(CreateMessageBusCmsRequest, msg);
    case MTYPE_CLIENT_SQS_REQUEST:
        return ClientActorRequest(CreateMessageBusSqsRequest, msg);
    case MTYPE_CLIENT_INTERCONNECT_DEBUG:
        return ClientActorRequest(CreateMessageBusInterconnectDebug, msg);
    case MTYPE_CLIENT_CONSOLE_REQUEST:
        return ClientActorRequest(CreateMessageBusConsoleRequest, msg);
    case MTYPE_CLIENT_TEST_SHARD_CONTROL:
        return ClientActorRequest(CreateMessageBusTestShardControl, msg);
    default:
        return UnknownMessage(msg);
    }
}

void TMessageBusServer::OnError(TAutoPtr<NBus::TBusMessage> msg, NBus::EMessageStatus status) {
    if (ActorSystem) {
        if (status == NBus::MESSAGE_SHUTDOWN) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::MSGBUS_REQUEST, "Msgbus client disconnected before reply was sent"
                    << " msg# " << msg->Describe());
        } else {
            LOG_ERROR_S(*ActorSystem, NKikimrServices::MSGBUS_REQUEST, "Failed to send reply over msgbus status# " << status
                    << " msg# " << msg->Describe());
        }
    }
}

template<typename TEv>
void TMessageBusServer::ClientProxyRequest(TBusMessageContext &msg) {
    if (Proxy)
        ActorSystem->Send(Proxy, new TEv(msg));
    else
        msg.SendReplyMove(new TBusResponseStatus(MSTATUS_ERROR, "MessageBus proxy is not available"));
}

void TMessageBusServer::ClientActorRequest(ActorCreationFunc func, TBusMessageContext &msg) {
    if (IActor *x = func(msg))
        ActorSystem->Register(x, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    else
        msg.SendReplyMove(new TBusResponseStatus(MSTATUS_ERROR));
}

void TMessageBusServer::GetTypes(TBusMessageContext &msg) {
    if (IActor *x = CreateMessageBusGetTypes(msg)) {
        ActorSystem->Register(x, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    } else {
        auto reply = new TBusTypesResponse();
        reply->Record.SetStatus(MSTATUS_ERROR);
        msg.SendReplyMove(reply);
    }
}

void TMessageBusServer::UnknownMessage(TBusMessageContext &msg) {
    msg.SendReplyMove(new TBusResponseStatus(MSTATUS_UNKNOWN, "undocumented error 9"));
}

IActor* TMessageBusServer::CreateProxy() {
    return CreateMessageBusServerProxy(this);
}

IActor* TMessageBusServer::CreateMessageBusTraceService() {
    return nullptr;
}

IMessageBusServer* CreateMsgBusServer(
    NBus::TBusMessageQueue *queue,
    const NBus::TBusServerSessionConfig &config,
    ui32 bindPort
) {
    return new TMessageBusServer(config, queue, bindPort);
}

}
}
