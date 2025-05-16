#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/util/address_classifier.h>

#include "actors/actors.h"
#include "kafka_connection.h"
#include "kafka_events.h"
#include "kafka_log_impl.h"
#include "kafka_metrics.h"

namespace NKafka {

using namespace NActors;
using namespace NKikimr;

static constexpr size_t HeaderSize = sizeof(TKafkaInt16) /* apiKey */ +
                                     sizeof(TKafkaVersion) /* version */ +
                                     sizeof(TKafkaInt32) /* correlationId */;

class TKafkaConnection: public TActorBootstrapped<TKafkaConnection>, public TNetworkConfig {
public:
    using TBase = TActorBootstrapped<TKafkaConnection>;

    struct Msg {
        using TPtr = std::shared_ptr<Msg>;

        size_t Size = 0;
        TKafkaInt32 ExpectedSize = 0;
        std::shared_ptr<TBuffer> Buffer = std::make_shared<TBuffer>(HeaderSize);

        TKafkaInt16 ApiKey;
        TKafkaVersion ApiVersion;
        TKafkaInt32 CorrelationId;

        TRequestHeaderData Header;
        TApiMessage::TPtr Message;

        TInstant StartTime;
        TString Method;

        TApiMessage::TPtr Response;
        EKafkaErrors ResponseErrorCode;
    };

    static constexpr TDuration InactivityTimeout = TDuration::Minutes(10);
    TEvPollerReady* InactivityEvent = nullptr;

    const TActorId ListenerActorId;

    TIntrusivePtr<TSocketDescriptor> Socket;
    TSocketAddressType Address;
    TPollerToken::TPtr PollerToken;
    TBufferedWriter Buffer;

    THPTimer InactivityTimer;

    bool IsAuthRequired = true;
    bool IsSslRequired = true;
    bool IsSslActive = false;

    bool ConnectionEstablished = false;
    bool CloseConnection = false;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier;

    std::shared_ptr<Msg> Request;
    std::unordered_map<ui64, Msg::TPtr> PendingRequests;
    std::deque<Msg::TPtr> PendingRequestsQueue;

    enum EReadSteps { SIZE_READ, SIZE_PREPARE, INFLIGTH_CHECK, HEADER_READ, HEADER_PROCESS, MESSAGE_READ, MESSAGE_PROCESS };
    EReadSteps Step;

    TReadDemand Demand;

    size_t InflightSize;

    TActorId ProduceActorId;
    TActorId ReadSessionActorId;

    TContext::TPtr Context;

    TKafkaConnection(const TActorId& listenerActorId,
                     TIntrusivePtr<TSocketDescriptor> socket,
                     TNetworkConfig::TSocketAddressType address,
                     const NKikimrConfig::TKafkaProxyConfig& config,
                     const TActorId& discoveryCacheActorId)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(address)
        , Buffer(Socket.Get(), config.GetPacketSize())
        , Step(SIZE_READ)
        , Demand(NoDemand)
        , InflightSize(0)
        , Context(std::make_shared<TContext>(config))
    {
        SetNonBlock();
        IsSslRequired = Socket->IsSslSupported();
        Context->DiscoveryCacheActor = discoveryCacheActorId;
    }

    void Bootstrap() {
        Context->ConnectionId = SelfId();
        Context->RequireAuthentication = NKikimr::AppData()->EnforceUserTokenRequirement;
        // if no authentication required, then we can use local database as our target
        if (!Context->RequireAuthentication) {
            Context->DatabasePath = NKikimr::AppData()->TenantName;
        }

        Become(&TKafkaConnection::StateAccepting);
        Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        KAFKA_LOG_I("incoming connection opened " << Address);
    }

    void PassAway() override {
        KAFKA_LOG_D("PassAway");

        ConnectionEstablished = false;
        if (ProduceActorId) {
            Send(ProduceActorId, new TEvents::TEvPoison());
        }
        if (ReadSessionActorId) {
            Send(ReadSessionActorId, new TEvents::TEvPoison());
        }
        Send(ListenerActorId, new TEvents::TEvUnsubscribe());
        Shutdown();
        TBase::PassAway();
    }

protected:
    void LogEvent(IEventHandle& ev) {
        KAFKA_LOG_T("Received event: " << ev.GetTypeName());
    }

    void SetNonBlock() noexcept {
        Socket->SetNonBlock();
    }

    void Shutdown() {
        KAFKA_LOG_D("Shutdown");

        PendingRequests.clear();
        PendingRequestsQueue.clear();

        if (Socket) {
            Socket->Shutdown();
        }
    }

    ssize_t SocketSend(const void* data, size_t size) {
        KAFKA_LOG_T("SocketSend Size=" << size);
        return Socket->Send(data, size);
    }

    ssize_t SocketReceive(void* data, size_t size) {
        return Socket->Receive(data, size);
    }

    void RequestPoller() {
        Socket->RequestPoller(PollerToken);
    }

    SOCKET GetRawSocket() const {
        return Socket->GetRawSocket();
    }

    TString LogPrefix() const {
        TStringBuilder sb;
        sb << "TKafkaConnection " << SelfId() << "(#" << GetRawSocket() << "," << Address->ToString() << ") State: ";
        auto stateFunc = CurrentStateFunc();
        if (stateFunc == &TKafkaConnection::StateConnected) {
            sb << "Connected ";
        } else if (stateFunc == &TKafkaConnection::StateAccepting) {
            sb << "Accepting ";
        } else {
            sb << "Unknown ";
        }
        return sb;
    }

    void SendRequestMetrics(const TActorContext& ctx) {
        ctx.Send(MakeKafkaMetricsServiceID(),
                    new TEvKafka::TEvUpdateCounter(1, BuildLabels(Context, Request->Method, "", "api.kafka.request.count", "")));
        ctx.Send(MakeKafkaMetricsServiceID(),
                        new TEvKafka::TEvUpdateCounter(Request->Size, BuildLabels(Context, Request->Method, "", "api.kafka.request.bytes", "")));
    }

    void SendResponseMetrics(const TString method, const TInstant requestStartTime, i32 bytes, EKafkaErrors errorCode, const TActorContext& ctx) {
        TDuration duration = TInstant::Now() - requestStartTime;
        ctx.Send(MakeKafkaMetricsServiceID(),
            new TEvKafka::TEvUpdateHistCounter(static_cast<i64>(duration.MilliSeconds()), 1, BuildLabels(Context, method, "", "api.kafka.response.duration_milliseconds", "")));
        ctx.Send(MakeKafkaMetricsServiceID(),
            new TEvKafka::TEvUpdateCounter(1, BuildLabels(Context, method, "", "api.kafka.response.count", ToString(errorCode))));
        ctx.Send(MakeKafkaMetricsServiceID(),
            new TEvKafka::TEvUpdateCounter(bytes, BuildLabels(Context, method, "", "api.kafka.response.bytes", "")));
    }

    void OnAccept() {
        InactivityTimer.Reset();
        TBase::Become(&TKafkaConnection::StateConnected);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    void HandleAccepting(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        if (!UpgradeToSecure()) {
            return;
        }
        OnAccept();
    }

    void HandleAccepting(NActors::TEvPollerReady::TPtr) {
        OnAccept();
    }

    STATEFN(StateAccepting) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleAccepting);
            hFunc(TEvPollerRegisterResult, HandleAccepting);
            HFunc(TEvKafka::TEvResponse, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
            default:
                KAFKA_LOG_ERROR("TKafkaConnection: Unexpected " << ev.Get()->GetTypeName());
        }
    }

    void HandleMessage(TRequestHeaderData* header, const TMessagePtr<TApiVersionsRequestData>& message) {
        Register(CreateKafkaApiVersionsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TProduceRequestData>& message, const TActorContext& ctx) {
        if (!ProduceActorId) {
            ProduceActorId = ctx.RegisterWithSameMailbox(CreateKafkaProduceActor(Context));
        }

        Send(ProduceActorId, new TEvKafka::TEvProduceRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSyncGroupRequestData>& message, const TActorContext& ctx) {
        if (!ReadSessionActorId) {
            ReadSessionActorId = ctx.RegisterWithSameMailbox(CreateKafkaReadSessionActor(Context, 0));
        }

        Send(ReadSessionActorId, new TEvKafka::TEvSyncGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<THeartbeatRequestData>& message, const TActorContext& ctx) {
        if (!ReadSessionActorId) {
            ReadSessionActorId = ctx.RegisterWithSameMailbox(CreateKafkaReadSessionActor(Context, 0));
        }

        Send(ReadSessionActorId, new TEvKafka::TEvHeartbeatRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TJoinGroupRequestData>& message, const TActorContext& ctx) {
        if (!ReadSessionActorId) {
            ReadSessionActorId = ctx.RegisterWithSameMailbox(CreateKafkaReadSessionActor(Context, 0));
        }

        Send(ReadSessionActorId, new TEvKafka::TEvJoinGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TLeaveGroupRequestData>& message, const TActorContext& ctx) {
        if (!ReadSessionActorId) {
            ReadSessionActorId = ctx.RegisterWithSameMailbox(CreateKafkaReadSessionActor(Context, 0));
        }

        Send(ReadSessionActorId, new TEvKafka::TEvLeaveGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TInitProducerIdRequestData>& message) {
        Register(CreateKafkaInitProducerIdActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(TRequestHeaderData* header, const TMessagePtr<TMetadataRequestData>& message) {
        Register(CreateKafkaMetadataActor(Context, header->CorrelationId, message, Context->DiscoveryCacheActor));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSaslAuthenticateRequestData>& message) {
        Register(CreateKafkaSaslAuthActor(Context, header->CorrelationId, Address, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSaslHandshakeRequestData>& message) {
        Register(CreateKafkaSaslHandshakeActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TListOffsetsRequestData>& message) {
        Register(CreateKafkaListOffsetsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TFetchRequestData>& message) {
        Register(CreateKafkaFetchActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TFindCoordinatorRequestData>& message) {
        Register(CreateKafkaFindCoordinatorActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TOffsetFetchRequestData>& message) {
        Register(CreateKafkaOffsetFetchActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TOffsetCommitRequestData>& message) {
        Register(CreateKafkaOffsetCommitActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TCreateTopicsRequestData>& message) {
        Register(CreateKafkaCreateTopicsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TCreatePartitionsRequestData>& message) {
        Register(CreateKafkaCreatePartitionsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TAlterConfigsRequestData>& message) {
        Register(CreateKafkaAlterConfigsActor(Context, header->CorrelationId, message));
    }

    template<class T>
    TMessagePtr<T> Cast(std::shared_ptr<Msg>& request) {
        return TMessagePtr<T>(request->Buffer, request->Message);
    }

    bool ProcessRequest(const TActorContext& ctx) {
        KAFKA_LOG_D("process message: ApiKey=" << Request->Header.RequestApiKey << ", ExpectedSize=" << Request->ExpectedSize
                                               << ", Size=" << Request->Size);

        auto apiKeyNameIt = EApiKeyNames.find(static_cast<EApiKey>(Request->Header.RequestApiKey));
        if (apiKeyNameIt == EApiKeyNames.end()) {
            KAFKA_LOG_ERROR("Unsupported message: ApiKey=" << Request->Header.RequestApiKey);
            PassAway();
            return false;
        }

        Request->Method = apiKeyNameIt->second;

        PendingRequestsQueue.push_back(Request);
        PendingRequests[Request->Header.CorrelationId] = Request;

        SendRequestMetrics(ctx);
        if (Request->Header.ClientId.has_value() && Request->Header.ClientId != "") {
            Context->KafkaClient = Request->Header.ClientId.value();
        }

        switch (Request->Header.RequestApiKey) {
            case PRODUCE:
                HandleMessage(&Request->Header, Cast<TProduceRequestData>(Request), ctx);
                break;

            case API_VERSIONS:
                HandleMessage(&Request->Header, Cast<TApiVersionsRequestData>(Request));
                break;

            case INIT_PRODUCER_ID:
                HandleMessage(&Request->Header, Cast<TInitProducerIdRequestData>(Request));
                break;

            case METADATA:
                HandleMessage(&Request->Header, Cast<TMetadataRequestData>(Request));
                break;

            case SASL_HANDSHAKE:
                HandleMessage(&Request->Header, Cast<TSaslHandshakeRequestData>(Request));
                break;

            case SASL_AUTHENTICATE:
                HandleMessage(&Request->Header, Cast<TSaslAuthenticateRequestData>(Request));
                break;

            case LIST_OFFSETS:
                HandleMessage(&Request->Header, Cast<TListOffsetsRequestData>(Request));
                break;

            case FETCH:
                HandleMessage(&Request->Header, Cast<TFetchRequestData>(Request));
                break;

            case JOIN_GROUP:
                HandleMessage(&Request->Header, Cast<TJoinGroupRequestData>(Request), ctx);
                break;

            case SYNC_GROUP:
                HandleMessage(&Request->Header, Cast<TSyncGroupRequestData>(Request), ctx);
                break;

            case LEAVE_GROUP:
                HandleMessage(&Request->Header, Cast<TLeaveGroupRequestData>(Request), ctx);
                break;

            case HEARTBEAT:
                HandleMessage(&Request->Header, Cast<THeartbeatRequestData>(Request), ctx);
                break;

            case FIND_COORDINATOR:
                HandleMessage(&Request->Header, Cast<TFindCoordinatorRequestData>(Request));
                break;

            case OFFSET_FETCH:
                HandleMessage(&Request->Header, Cast<TOffsetFetchRequestData>(Request));
                break;

            case OFFSET_COMMIT:
                HandleMessage(&Request->Header, Cast<TOffsetCommitRequestData>(Request));
                break;

            case CREATE_TOPICS:
                HandleMessage(&Request->Header, Cast<TCreateTopicsRequestData>(Request));
                break;

            case CREATE_PARTITIONS:
                HandleMessage(&Request->Header, Cast<TCreatePartitionsRequestData>(Request));
                break;

            case ALTER_CONFIGS:
                HandleMessage(&Request->Header, Cast<TAlterConfigsRequestData>(Request));
                break;

            default:
                KAFKA_LOG_ERROR("Unsupported message: ApiKey=" << Request->Header.RequestApiKey);
                PassAway();
                return false;
        }

        // Now message and buffer are held by message actor
        Request->Message.reset();
        Request->Buffer.reset();

        Request.reset();

        return true;
    }

    void Handle(TEvKafka::TEvResponse::TPtr response, const TActorContext& ctx) {
        auto r = response->Get();
        Reply(r->CorrelationId, r->Response, r->ErrorCode, ctx);
    }

    void Handle(TEvKafka::TEvReadSessionInfo::TPtr readInfo, const TActorContext& /*ctx*/) {
        auto r = readInfo->Get();
        Context->GroupId = r->GroupId;
    }

    void Handle(TEvKafka::TEvAuthResult::TPtr ev, const TActorContext& ctx) {
        auto event = ev->Get();

        auto authStep = event->AuthStep;
        if (authStep == EAuthSteps::FAILED) {
            KAFKA_LOG_ERROR(event->Error);
            Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);
            CloseConnection = true;
            return;
        }

        Context->RequireAuthentication = NKikimr::AppData()->EnforceUserTokenRequirement;
        Context->UserToken = event->UserToken;
        Context->DatabasePath = event->DatabasePath;
        Context->AuthenticationStep = authStep;
        Context->RlContext = {event->Coordinator, event->ResourcePath, event->DatabasePath, event->UserToken->GetSerializedToken()};
        Context->DatabaseId = event->DatabaseId;
        Context->CloudId = event->CloudId;
        Context->FolderId = event->FolderId;
        Context->IsServerless = event->IsServerless;

        KAFKA_LOG_D("Authentificated successful. SID=" << Context->UserToken->GetUserSID());
        Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);
    }

    void Handle(TEvKafka::TEvHandshakeResult::TPtr ev, const TActorContext& ctx) {
        auto event = ev->Get();
        Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);

        auto authStep = event->AuthStep;
        if (authStep == EAuthSteps::FAILED) {
            KAFKA_LOG_ERROR(event->Error);
            CloseConnection = true;
            return;
        }

        Context->SaslMechanism = event->SaslMechanism;
        Context->AuthenticationStep = authStep;
    }

    void HandleKillReadSession() {
        if (ReadSessionActorId) {
            Send(ReadSessionActorId, new TEvents::TEvPoison());

            TActorId emptyActor;
            ReadSessionActorId = emptyActor;
        }
    }

    void Reply(const ui64 correlationId, TApiMessage::TPtr response, EKafkaErrors errorCode, const TActorContext& ctx) {
        auto it = PendingRequests.find(correlationId);
        if (it == PendingRequests.end()) {
            KAFKA_LOG_ERROR("Unexpected correlationId " << correlationId);
            return;
        }

        auto& request = it->second;
        request->Response = response;
        request->ResponseErrorCode = errorCode;

        if (!ProcessReplyQueue(ctx)) {
            return;
        }
        RequestPoller();
    }

    bool ProcessReplyQueue(const TActorContext& ctx) {
        while(!PendingRequestsQueue.empty()) {
            auto& request = PendingRequestsQueue.front();
            if (request->Response.get() == nullptr) {
                break;
            }

            if (!Reply(&request->Header, request->Response.get(), request->Method, request->StartTime, request->ResponseErrorCode, ctx)) {
                return false;
            }

            InflightSize -= request->ExpectedSize;

            PendingRequests.erase(request->Header.CorrelationId);
            PendingRequestsQueue.pop_front();
        }

        return true;
    }

    bool Reply(const TRequestHeaderData* header, const TApiMessage* reply, const TString method, const TInstant requestStartTime, EKafkaErrors errorCode, const TActorContext& ctx) {
        TKafkaVersion headerVersion = ResponseHeaderVersion(header->RequestApiKey, header->RequestApiVersion);
        TKafkaVersion version = header->RequestApiVersion;

        TResponseHeaderData responseHeader;
        responseHeader.CorrelationId = header->CorrelationId;

        TKafkaInt32 size = responseHeader.Size(headerVersion) + reply->Size(version);

        TKafkaWritable writable(Buffer);
        SendResponseMetrics(method, requestStartTime, size, errorCode, ctx);
        try {
            writable << size;
            responseHeader.Write(writable, headerVersion);
            reply->Write(writable, version);

            ssize_t res = Buffer.flush();
            if (res < 0) {
                ythrow yexception() << "Error during flush of the written to socket data. Error code: " << strerror(-res) << " (" << res << ")";
            }

            KAFKA_LOG_D("Sent reply: ApiKey=" << header->RequestApiKey << ", Version=" << version << ", Correlation=" << responseHeader.CorrelationId <<  ", Size=" << size);
        } catch(const yexception& e) {
            KAFKA_LOG_ERROR("error on processing response: ApiKey=" << reply->ApiKey()
                                                     << ", Version=" << version
                                                     << ", CorrelationId=" << header->CorrelationId
                                                     << ", Error=" <<  e.what());
            PassAway();
            return false;
        }

        return true;
    }

    bool UpgradeToSecure() {
        if (IsSslRequired && !IsSslActive) {
            int res = Socket->TryUpgradeToSecure();
            if (res < 0) {
                KAFKA_LOG_ERROR("connection closed - error in UpgradeToSecure: " << strerror(-res));
                PassAway();
                return false;
            }
            IsSslActive = true;
        }
        return true;
    }

    bool DoRead(const TActorContext& ctx) {
        KAFKA_LOG_T("DoRead: Demand=" << Demand.Length << ", Step=" << static_cast<i32>(Step));
        for (;;) {
            while (Demand) {
                ssize_t received = 0;
                ssize_t res = SocketReceive(Demand.Buffer, Demand.GetLength());
                if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    return true;
                } else if (-res == EINTR) {
                    continue;
                } else if (!res) {
                    KAFKA_LOG_I("connection closed");
                    PassAway();
                    return false;
                } else if (res < 0) {
                    KAFKA_LOG_I("connection closed - error in recv: " << strerror(-res));
                    PassAway();
                    return false;
                }
                received = res;

                Request->Size += received;
                Demand.Buffer += received;
                Demand.Length -= received;
            }
            if (!Demand) {
                switch (Step) {
                    case SIZE_READ:
                        Request = std::make_shared<Msg>();
                        Demand = TReadDemand((char*)&(Request->ExpectedSize), sizeof(Request->ExpectedSize));
                        Step = SIZE_PREPARE;
                        break;

                    case SIZE_PREPARE:
                        NormalizeNumber(Request->ExpectedSize);
                        if (Request->ExpectedSize < 0) {
                            KAFKA_LOG_ERROR("Wrong message size. Size: " << Request->ExpectedSize);
                            PassAway();
                            return false;
                        }
                        if ((ui64)Request->ExpectedSize > Context->Config.GetMaxMessageSize()) {
                            KAFKA_LOG_ERROR("message is big. Size: " << Request->ExpectedSize << ". MaxSize: "
                                                                     << Context->Config.GetMaxMessageSize());
                            PassAway();
                            return false;
                        }
                        if (static_cast<size_t>(Request->ExpectedSize) < HeaderSize) {
                            KAFKA_LOG_ERROR("message is small. Size: " << Request->ExpectedSize);
                            PassAway();
                            return false;
                        }

                        Step = INFLIGTH_CHECK;

                        [[fallthrough]];

                    case INFLIGTH_CHECK:
                        if (!Context->Authenticated() && !PendingRequestsQueue.empty()) {
                            // Allow only one message to be processed at a time for non-authenticated users
                            KAFKA_LOG_ERROR("DoRead: failed inflight check: there are " << PendingRequestsQueue.size() << " pending requests and user is not authnicated.  Only one paraller request is allowed for a non-authenticated user.");
                            return true;
                        }
                        if (InflightSize + Request->ExpectedSize > Context->Config.GetMaxInflightSize()) {
                            // We limit the size of processed messages so as not to exceed the size of available memory
                            KAFKA_LOG_ERROR("DoRead: failed inflight check: InflightSize + Request->ExpectedSize=" << InflightSize + Request->ExpectedSize << " > Context->Config.GetMaxInflightSize=" << Context->Config.GetMaxInflightSize());
                            return true;
                        }
                        InflightSize += Request->ExpectedSize;
                        Step = MESSAGE_READ;

                        [[fallthrough]];

                    case HEADER_READ:
                        KAFKA_LOG_T("start read header. ExpectedSize=" << Request->ExpectedSize);

                        Request->Buffer->Resize(HeaderSize);
                        Demand = TReadDemand(Request->Buffer->Data(), HeaderSize);

                        Step = HEADER_PROCESS;
                        break;

                    case HEADER_PROCESS:
                        Request->ApiKey = *(TKafkaInt16*)Request->Buffer->Data();
                        Request->ApiVersion = *(TKafkaVersion*)(Request->Buffer->Data() + sizeof(TKafkaInt16));
                        Request->CorrelationId = *(TKafkaInt32*)(Request->Buffer->Data() + sizeof(TKafkaInt16) + sizeof(TKafkaVersion));

                        NormalizeNumber(Request->ApiKey);
                        NormalizeNumber(Request->ApiVersion);
                        NormalizeNumber(Request->CorrelationId);

                        if (PendingRequests.contains(Request->CorrelationId)) {
                            KAFKA_LOG_ERROR("CorrelationId " << Request->CorrelationId << " already processing");
                            PassAway();
                            return false;
                        }
                        if (!Context->Authenticated() && RequireAuthentication(static_cast<EApiKey>(Request->ApiKey))) {
                            KAFKA_LOG_ERROR("unauthenticated request: ApiKey=" << Request->ApiKey);
                            PassAway();
                            return false;
                        }

                        Step = MESSAGE_READ;

                        [[fallthrough]];

                    case MESSAGE_READ:
                        KAFKA_LOG_T("start read new message. ExpectedSize=" << Request->ExpectedSize);

                        Request->Buffer->Resize(Request->ExpectedSize);
                        Demand = TReadDemand(Request->Buffer->Data() + HeaderSize, Request->ExpectedSize - HeaderSize);

                        Step = MESSAGE_PROCESS;
                        break;

                    case MESSAGE_PROCESS:
                        Request->StartTime = TInstant::Now();
                        KAFKA_LOG_D("received message. ApiKey=" << Request->ApiKey << ", Version=" << Request->ApiVersion << ", CorrelationId=" << Request->CorrelationId);

                        TKafkaReadable readable(*Request->Buffer);

                        try {
                            Request->Message = CreateRequest(Request->ApiKey);

                            Request->Header.Read(readable, RequestHeaderVersion(Request->ApiKey, Request->ApiVersion));
                            Request->Message->Read(readable, Request->ApiVersion);
                        } catch(const yexception& e) {
                            KAFKA_LOG_ERROR("error on processing message: ApiKey=" << Request->ApiKey
                                                                    << ", Version=" << Request->ApiVersion
                                                                    << ", CorrelationId=" << Request->CorrelationId
                                                                    << ", Error=" <<  e.what());
                            PassAway();
                            return false;
                        }

                        Step = SIZE_READ;

                        if (!ProcessRequest(ctx)) {
                            return false;
                        }

                        break;
                }
            }
        }
    }

    bool RequireAuthentication(EApiKey apiKey) {
        return !(EApiKey::API_VERSIONS == apiKey || 
                EApiKey::SASL_HANDSHAKE == apiKey || 
                EApiKey::SASL_AUTHENTICATE == apiKey);
    }


    void HandleConnected(TEvPollerReady::TPtr event, const TActorContext& ctx) {
        if (event->Get()->Read) {
            if (!CloseConnection) {
                if (!DoRead(ctx)) {
                    return;
                }
            }

            if (event->Get() == InactivityEvent) {
                const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
                if (passed >= InactivityTimeout) {
                    KAFKA_LOG_D("connection closed by inactivity timeout");
                    return PassAway(); // timeout
                } else {
                    Schedule(InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
                }
            }
        }
        if (event->Get()->Write) {
            ssize_t res = Buffer.flush();
            if (res < 0) {
                KAFKA_LOG_ERROR("connection closed - error in FlushOutput: " << strerror(-res));
                PassAway();
                return;
            }
        }

        if (CloseConnection && Buffer.Empty()) {
            KAFKA_LOG_D("connection closed");
            return PassAway();
        }

        RequestPoller();
    }

    void HandleConnected(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        if (!UpgradeToSecure()) {
            return;
        }
        PollerToken->Request(true, true);
    }

    STATEFN(StateConnected) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPollerReady, HandleConnected);
            hFunc(TEvPollerRegisterResult, HandleConnected);
            HFunc(TEvKafka::TEvResponse, Handle);
            HFunc(TEvKafka::TEvAuthResult, Handle);
            HFunc(TEvKafka::TEvReadSessionInfo, Handle);
            HFunc(TEvKafka::TEvHandshakeResult, Handle);
            sFunc(TEvKafka::TEvKillReadSession, HandleKillReadSession);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
            default:
                KAFKA_LOG_ERROR("TKafkaConnection: Unexpected " << ev.Get()->GetTypeName());
        }
    }
};

NActors::IActor* CreateKafkaConnection(const TActorId& listenerActorId,
                                       TIntrusivePtr<TSocketDescriptor> socket,
                                       TNetworkConfig::TSocketAddressType address,
                                       const NKikimrConfig::TKafkaProxyConfig& config,
                                       const TActorId& discoveryCacheActorId) {
    return new TKafkaConnection(listenerActorId, std::move(socket), std::move(address), config, discoveryCacheActorId);
}

} // namespace NKafka
