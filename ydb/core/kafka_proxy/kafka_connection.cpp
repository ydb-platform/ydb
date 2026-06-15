#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/raw_socket/sock_config.h>
#include <ydb/core/util/address_classifier.h>
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/core/kafka_proxy/actors/kafka_balancer_actor.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_actor.h>


#include "actors/actors.h"
#include "kafka_connection.h"
#include "kafka_events.h"
#include <ydb/library/kafka/kafka_log.h>
#include "kafka_metrics.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KAFKA_PROXY

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
    TBufferedWriter<> BufferedWriter;

    THPTimer InactivityTimer;

    bool IsAuthRequired = true;
    bool IsSslRequired = true;
    bool IsSslActive = false;

    bool ConnectionEstablished = false;
    bool CloseConnection = false;

    bool RetryingWriteToSocket = false;
    TEvPollerReady::TPtr PollerEventSaved = nullptr;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier;

    std::shared_ptr<Msg> Request;
    std::unordered_map<ui64, Msg::TPtr> PendingRequests;
    std::deque<Msg::TPtr> PendingRequestsQueue;

    enum EReadSteps { SIZE_READ, SIZE_PREPARE, INFLIGHT_CHECK, HEADER_READ, HEADER_PROCESS, MESSAGE_READ, MESSAGE_PROCESS };
    EReadSteps Step;

    TReadDemand Demand;

    size_t InflightSize;

    TActorId ProduceActorId;
    TActorId AuthActorId;

    enum MtlsAuthStages { NO_CERT_YET, PROCESSING_CERT, AUTH_SUCCESSFUL, AUTH_FAILED };
    MtlsAuthStages MtlsAuthStage;
    std::shared_ptr<TInet64SecureStreamSocket::TServerMtlsCreds> ServerCreds;

    enum SslHandshakeErrors {ERROR_NONE = 0, ERROR_WANT_READ = 1, ERROR_WANT_WRITE = 2};

    TContext::TPtr Context;

    TKafkaConnection(const TActorId& listenerActorId,
                     TIntrusivePtr<TSocketDescriptor> socket,
                     TNetworkConfig::TSocketAddressType address,
                     const NKikimrConfig::TKafkaProxyConfig& config,
                     std::shared_ptr<NKafka::TInet64SecureStreamSocket::TServerMtlsCreds> serverCreds)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(address)
        , BufferedWriter(Socket.Get(), config.GetPacketSize())
        , Step(SIZE_READ)
        , Demand(NoDemand)
        , InflightSize(0)
        , ServerCreds(serverCreds)
        , Context(std::make_shared<TContext>(config))
    {
        SetNonBlock();
        IsSslRequired = Socket->IsSslSupported();
    }

    void Bootstrap() {
        Context->ConnectionId = SelfId();
        Context->RequireAuthentication = NKikimr::AppData()->EnforceUserTokenRequirement || NKikimr::AppData()->PQConfig.GetRequireCredentialsInNewProtocol();
        // if no authentication required, then we can use local database as our target
        if (!Context->RequireAuthentication) {
            Context->DatabasePath = NKikimr::AppData()->TenantName;
            Context->ResourceDatabasePath = NKikimr::AppData()->TenantName;
        }

        MtlsAuthStage = NO_CERT_YET;

        Become(&TKafkaConnection::StateAccepting);
        Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        YDB_LOG_INFO("Incoming connection opened",
            {"logPrefix", LogPrefix()},
            {"address", Address});
    }

    void PassAway() override {
        YDB_LOG_DEBUG("PassAway",
            {"logPrefix", LogPrefix()});

        ConnectionEstablished = false;
        if (ProduceActorId) {
            Send(ProduceActorId, new TEvents::TEvPoison());
        }
        if (AuthActorId) {
            Send(AuthActorId, new TEvents::TEvPoison());
        }
        if (Context->ReadSession.ProxyActorId) {
            Send(Context->ReadSession.ProxyActorId, new TEvents::TEvPoison());
        }
        Send(ListenerActorId, new TEvents::TEvUnsubscribe());
        Shutdown();
        TBase::PassAway();
    }

protected:
    void LogEvent(IEventHandle& ev) {
        YDB_LOG_TRACE("Received",
            {"logPrefix", LogPrefix()},
            {"event", ev.GetTypeName()});
    }

    void SetNonBlock() noexcept {
        Socket->SetNonBlock();
    }

    void Shutdown() {
        YDB_LOG_DEBUG("Shutdown",
            {"logPrefix", LogPrefix()});

        PendingRequests.clear();
        PendingRequestsQueue.clear();

        if (Socket) {
            Socket->Shutdown();
        }
    }

    ssize_t SocketSend(const void* data, size_t size) {
        YDB_LOG_TRACE("SocketSend",
            {"logPrefix", LogPrefix()},
            {"size", size});
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
                YDB_LOG_ERROR("TKafkaConnection: Unexpected",
                    {"logPrefix", LogPrefix()},
                    {"typeName", ev.Get()->GetTypeName()});
        }
    }

    void HandleMessage(TRequestHeaderData* header, const TMessagePtr<TApiVersionsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaApiVersionsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TProduceRequestData>& message, const TActorContext& ctx) {
        if (!ProduceActorId) {
            ProduceActorId = ctx.RegisterWithSameMailbox(CreateKafkaProduceActor(Context));
        }

        Send(ProduceActorId, new TEvKafka::TEvProduceRequest(header->CorrelationId, message));
    }

    void EnsureReadSessionActor() {
        if (!Context->ReadSession.ProxyActorId) {
            Context->ReadSession.ProxyActorId = RegisterWithSameMailbox(CreateKafkaReadSessionProxyActor(Context, 0));
        }
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TJoinGroupRequestData>& message, const TActorContext& /*ctx*/) {
        EnsureReadSessionActor();
        Send(Context->ReadSession.ProxyActorId, new TEvKafka::TEvJoinGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSyncGroupRequestData>& message, const TActorContext& /*ctx*/) {
        EnsureReadSessionActor();
        Send(Context->ReadSession.ProxyActorId, new TEvKafka::TEvSyncGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<THeartbeatRequestData>& message, const TActorContext& /*ctx*/) {
        EnsureReadSessionActor();
        Send(Context->ReadSession.ProxyActorId, new TEvKafka::TEvHeartbeatRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TLeaveGroupRequestData>& message, const TActorContext& /*ctx*/) {
        EnsureReadSessionActor();
        Send(Context->ReadSession.ProxyActorId, new TEvKafka::TEvLeaveGroupRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TInitProducerIdRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaInitProducerIdActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(TRequestHeaderData* header, const TMessagePtr<TMetadataRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaMetadataActor(Context, header->CorrelationId, message, NKafka::MakeKafkaDiscoveryCacheID()));
    }

    void HandleMessage(TRequestHeaderData* header, const TMessagePtr<TDescribeConfigsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaDescribeConfigsActor(Context, header->CorrelationId, message));
    }

    void EnsureKafkaSaslAuthActor() {
        if (!AuthActorId) {
            AuthActorId = RegisterWithSameMailbox(CreateKafkaSaslAuthActor(Context, Address));
        }
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSaslAuthenticateRequestData>& message) {
        EnsureKafkaSaslAuthActor();
        Send(AuthActorId, new TEvKafka::TEvAuthRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TSaslHandshakeRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaSaslHandshakeActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TListOffsetsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaListOffsetsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TDescribeGroupsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaDescribeGroupsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TListGroupsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaListGroupsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TFetchRequestData>& message) {
        EnsureReadSessionActor();
        Send(Context->ReadSession.ProxyActorId, new TEvKafka::TEvFetchRequest(header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TFindCoordinatorRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaFindCoordinatorActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TOffsetFetchRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaOffsetFetchActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TOffsetCommitRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaOffsetCommitActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TCreateTopicsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaCreateTopicsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TCreatePartitionsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaCreatePartitionsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TAlterConfigsRequestData>& message) {
        RegisterWithSameMailbox(CreateKafkaAlterConfigsActor(Context, header->CorrelationId, message));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TAddPartitionsToTxnRequestData>& message) {
        Send(MakeTransactionsServiceID(SelfId().NodeId()), new TEvKafka::TEvAddPartitionsToTxnRequest(
            header->CorrelationId,
            message,
            Context->ConnectionId,
            Context->DatabasePath,
            Context->ResourceDatabasePath
        ));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TAddOffsetsToTxnRequestData>& message) {
        Send(MakeTransactionsServiceID(SelfId().NodeId()), new TEvKafka::TEvAddOffsetsToTxnRequest(
            header->CorrelationId,
            message,
            Context->ConnectionId,
            Context->DatabasePath,
            Context->ResourceDatabasePath
        ));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TTxnOffsetCommitRequestData>& message) {
        Send(MakeTransactionsServiceID(SelfId().NodeId()), new TEvKafka::TEvTxnOffsetCommitRequest(
            header->CorrelationId,
            message,
            Context->ConnectionId,
            Context->DatabasePath,
            Context->ResourceDatabasePath
        ));
    }

    void HandleMessage(const TRequestHeaderData* header, const TMessagePtr<TEndTxnRequestData>& message) {
        Send(MakeTransactionsServiceID(SelfId().NodeId()), new TEvKafka::TEvEndTxnRequest(
            header->CorrelationId,
            message,
            Context->ConnectionId,
            Context->DatabasePath,
            Context->ResourceDatabasePath
        ));
    }

    template<class T>
    TMessagePtr<T> Cast(std::shared_ptr<Msg>& request) {
        return TMessagePtr<T>(request->Buffer, request->Message);
    }

    bool ProcessRequest(const TActorContext& ctx) {
        YDB_LOG_DEBUG("Process message",
            {"logPrefix", LogPrefix()},
            {"apiKey", Request->Header.RequestApiKey},
            {"expectedSize", Request->ExpectedSize},
            {"size", Request->Size});

        auto apiKeyNameIt = EApiKeyNames.find(static_cast<EApiKey>(Request->Header.RequestApiKey));
        if (apiKeyNameIt == EApiKeyNames.end()) {
            YDB_LOG_ERROR("Unsupported message",
                {"logPrefix", LogPrefix()},
                {"apiKey", Request->Header.RequestApiKey});
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

        if (IsTransactionalApiKey(Request->Header.RequestApiKey) && !TransactionsEnabled()) {
            YDB_LOG_ERROR("Transactional API keys are not enabled. To enable them set \"EnableKafkaTransactions\" feature flag to true in cluster configuration",
                {"logPrefix", LogPrefix()});
            PassAway();
            return false;
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

            case LIST_GROUPS:
                HandleMessage(&Request->Header, Cast<TListGroupsRequestData>(Request));
                break;
            case DESCRIBE_GROUPS:
                HandleMessage(&Request->Header, Cast<TDescribeGroupsRequestData>(Request));
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

            case DESCRIBE_CONFIGS:
                HandleMessage(&Request->Header, Cast<TDescribeConfigsRequestData>(Request));
                break;

            case CREATE_PARTITIONS:
                HandleMessage(&Request->Header, Cast<TCreatePartitionsRequestData>(Request));
                break;

            case ALTER_CONFIGS:
                HandleMessage(&Request->Header, Cast<TAlterConfigsRequestData>(Request));
                break;

            case ADD_PARTITIONS_TO_TXN:
                HandleMessage(&Request->Header, Cast<TAddPartitionsToTxnRequestData>(Request));
                break;

            case ADD_OFFSETS_TO_TXN:
                HandleMessage(&Request->Header, Cast<TAddOffsetsToTxnRequestData>(Request));
                break;

            case TXN_OFFSET_COMMIT:
                HandleMessage(&Request->Header, Cast<TTxnOffsetCommitRequestData>(Request));
                break;

            case END_TXN:
                HandleMessage(&Request->Header, Cast<TEndTxnRequestData>(Request));
                break;

            default:
                YDB_LOG_ERROR("Unsupported message",
                    {"logPrefix", LogPrefix()},
                    {"apiKey", Request->Header.RequestApiKey});
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
        YDB_LOG_DEBUG("Initializing",
            {"logPrefix", LogPrefix()},
            {"groupId", r->GroupId});
        Context->GroupId = r->GroupId;
    }

    void Handle(TEvKafka::TEvAuthResult::TPtr ev, const TActorContext& ctx) {
        auto event = ev->Get();

        auto authStep = event->AuthStep;
        if (authStep == EAuthSteps::FAILED) {
            if (IsSslActive && NKikimr::AppData()->KafkaProxyConfig.GetMtlsEnable()) {
                MtlsAuthStage = AUTH_FAILED;
            }
            YDB_LOG_ERROR("",
                {"logPrefix", LogPrefix()},
                {"error", event->Error});
            Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);
            CloseConnection = true;
            return;
        }

        if (Context->SaslMechanism != "MTLS" && IsSslActive && NKikimr::AppData()->KafkaProxyConfig.GetMtlsEnable()) {
            auto kafkaError = EKafkaErrors::SASL_AUTHENTICATION_FAILED;
            auto responseToClient = std::make_shared<TSaslAuthenticateResponseData>();
            responseToClient->ErrorCode = kafkaError;
            TString errorMessage = TStringBuilder() << Context->SaslMechanism << " authentication mechanism is disabled, because mTLS flag is on. Turn of mTLS in configuration to use this mechanism.";
            responseToClient->ErrorMessage = errorMessage;
            YDB_LOG_DEBUG("Dump logPrefix, errorMessage",
                {"logPrefix", LogPrefix()},
                {"errorMessage", errorMessage});

            std::shared_ptr<TEvKafka::TEvResponse> errorResponse = std::make_shared<TEvKafka::TEvResponse>(event->ClientResponse->CorrelationId, responseToClient, kafkaError);
            Reply(event->ClientResponse->CorrelationId, errorResponse->Response, kafkaError, ctx);
            CloseConnection = true;
            return;
        }

        Context->RequireAuthentication = NKikimr::AppData()->EnforceUserTokenRequirement || NKikimr::AppData()->PQConfig.GetRequireCredentialsInNewProtocol();
        Context->UserToken = event->UserToken;
        Context->DatabasePath = event->DatabasePath;
        Context->AuthenticationStep = authStep;
        Context->RlContext = {event->Coordinator, event->ResourcePath, event->DatabasePath, event->UserToken->GetSerializedToken()};
        Context->DatabaseId = event->DatabaseId;
        Context->CloudId = event->CloudId;
        Context->FolderId = event->FolderId;
        Context->IsServerless = event->IsServerless;
        Context->ResourceDatabasePath = event->ResourceDatabasePath ? NKikimr::CanonizePath(event->ResourceDatabasePath) : Context->DatabasePath;

        YDB_LOG_DEBUG("Authentication successful",
            {"logPrefix", LogPrefix()},
            {"SID", Context->UserToken->GetUserSID()});
        if (Context->SaslMechanism != "MTLS") {
            Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);
        } else {
            MtlsAuthStage = AUTH_SUCCESSFUL;
            HandleConnected(PollerEventSaved, ctx);
        }
    }

    void Handle(TEvKafka::TEvHandshakeResult::TPtr ev, const TActorContext& ctx) {
        auto event = ev->Get();

        if (IsSslActive && NKikimr::AppData()->KafkaProxyConfig.GetMtlsEnable()) {
            EKafkaErrors kafkaError = EKafkaErrors::SASL_AUTHENTICATION_FAILED;
            auto responseToClient = std::make_shared<TSaslHandshakeResponseData>();
            responseToClient->ErrorCode = kafkaError;

            auto errorResponse = std::make_shared<TEvKafka::TEvResponse>(event->ClientResponse->CorrelationId, responseToClient, kafkaError);
            TString errorMessage = TStringBuilder() << event->SaslMechanism << " authentication mechanism is disabled, because mTLS flag is on. Turn of mTLS in configuration to use this mechanism.";
            YDB_LOG_DEBUG("Dump logPrefix, errorMessage",
                {"logPrefix", LogPrefix()},
                {"errorMessage", errorMessage});
            Reply(event->ClientResponse->CorrelationId, errorResponse->Response, kafkaError, ctx);
            CloseConnection = true;
            return;
        }

        Reply(event->ClientResponse->CorrelationId, event->ClientResponse->Response, event->ClientResponse->ErrorCode, ctx);
        auto authStep = event->AuthStep;
        if (authStep == EAuthSteps::FAILED) {
            YDB_LOG_ERROR("",
                {"logPrefix", LogPrefix()},
                {"error", event->Error});
            CloseConnection = true;
            return;
        }

        Context->SaslMechanism = event->SaslMechanism;
        Context->AuthenticationStep = authStep;
    }

    void HandleKillReadSession() {
        if (Context->ReadSession.ProxyActorId) {
            Send(Context->ReadSession.ProxyActorId, new TEvents::TEvPoison());

            Context->ReadSession.ProxyActorId = {};
        }
    }

    void Reply(const ui64 correlationId, TApiMessage::TPtr response, EKafkaErrors errorCode, const TActorContext& ctx) {
        auto it = PendingRequests.find(correlationId);
        if (it == PendingRequests.end()) {
            YDB_LOG_ERROR("Unexpected correlationId",
                {"logPrefix", LogPrefix()},
                {"correlationId", correlationId});
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

    void OnRequestProcessed(const Msg::TPtr& request) {
        YDB_LOG_TRACE("Request with correlationId processed. Erasing it from PendingRequests and PendingRequestsQueue",
            {"logPrefix", LogPrefix()},
            {"correlationId", request->Header.CorrelationId});
        InflightSize -= request->ExpectedSize;
        PendingRequests.erase(request->Header.CorrelationId);
        PendingRequestsQueue.pop_front();
    }

    bool ProcessReplyQueue(const TActorContext& ctx) {
        while(!PendingRequestsQueue.empty()) {
            auto& request = PendingRequestsQueue.front();
            YDB_LOG_TRACE("Processing reply queue for request with correlationId",
                {"logPrefix", LogPrefix()},
                {"correlationId", request->Header.CorrelationId});
            if (request->Response.get() == nullptr) {
                YDB_LOG_TRACE("Response for request with correlationId is empty",
                    {"logPrefix", LogPrefix()},
                    {"correlationId", request->Header.CorrelationId});
                break;
            }

            if (RetryingWriteToSocket || !Reply(&request->Header, request->Response.get(), request->Method, request->StartTime, request->ResponseErrorCode, ctx)) {
                return false;
            }

            OnRequestProcessed(request);
        }

        if (!CloseConnection && Step == INFLIGHT_CHECK) {
            DoRead(ctx);
        }

        return true;
    }

    bool Reply(const TRequestHeaderData* header, const TApiMessage* reply, const TString method, const TInstant requestStartTime, EKafkaErrors errorCode, const TActorContext& ctx) {
        YDB_LOG_TRACE("Building reply for method and correlationId with error",
            {"logPrefix", LogPrefix()},
            {"method", method},
            {"correlationId", header->CorrelationId},
            {"code", errorCode});
        TKafkaVersion headerVersion = ResponseHeaderVersion(header->RequestApiKey, header->RequestApiVersion);
        TKafkaVersion version = header->RequestApiVersion;

        TResponseHeaderData responseHeader;
        responseHeader.CorrelationId = header->CorrelationId;

        TKafkaInt32 size = responseHeader.Size(headerVersion) + reply->Size(version);
        SendResponseMetrics(method, requestStartTime, size, errorCode, ctx);
        try {
            TKafkaWriteBuffer replyBuffer(Context->Config.GetPacketSize());
            TKafkaWritable writable(replyBuffer);
            writable << size;
            responseHeader.Write(writable, headerVersion);
            reply->Write(writable, version);

            for (auto it = replyBuffer.GetBuffersDeque().rbegin(); it != replyBuffer.GetBuffersDeque().rend(); ++it) {
                BufferedWriter.write(it->Data(), it->Size());
            }

            ssize_t res = BufferedWriter.flush();
            // if we got EAGAIN or EWOULDBLOCK it means that socket is busy and we need to wait for PollerReady event to proceed
            // PollerReady means that poller polled socket ready status
            if (res == -EAGAIN || res == -EWOULDBLOCK) {
                RetryingWriteToSocket = true;
                YDB_LOG_DEBUG("Socket is busy. Buffer queue Waiting for PollerReady event",
                    {"logPrefix", LogPrefix()},
                    {"size", BufferedWriter.GetBuffersDeque().size()});
                RequestPoller();
                return false;
            } else if (res < 0) {
                ythrow yexception() << "Error during flush of the written to socket data. Error code: " << strerror(-res) << " (" << res << ")";
            } else {
                YDB_LOG_DEBUG("Sent reply",
                    {"logPrefix", LogPrefix()},
                    {"apiKey", header->RequestApiKey},
                    {"version", version},
                    {"correlation", responseHeader.CorrelationId},
                    {"size", size});
            }
        } catch(const yexception& e) {
            YDB_LOG_ERROR("Error on processing response",
                {"logPrefix", LogPrefix()},
                {"apiKey", reply->ApiKey()},
                {"version", version},
                {"correlationId", header->CorrelationId},
                {"error", e.what()});
            PassAway();
            return false;
        }

        return true;
    }

    bool UpgradeToSecure() {
        if (IsSslRequired && !IsSslActive) {
            int res = Socket->TryUpgradeToSecure(NKikimrServices::KAFKA_PROXY, ServerCreds);
            if (res < 0) {
                YDB_LOG_ERROR("Connection closed - error",
                    {"logPrefix", LogPrefix()},
                    {"upgradeToSecure", strerror(-res)});
                PassAway();
                return false;
            }
            IsSslActive = true;
        }
        return true;
    }

    bool DoRead(const TActorContext& ctx) {
        YDB_LOG_TRACE("DoRead",
            {"logPrefix", LogPrefix()},
            {"demand", Demand.Length},
            {"step", static_cast<i32>(Step)});
        for (;;) {
            while (Demand) {
                ssize_t received = 0;
                ssize_t res = SocketReceive(Demand.Buffer, Demand.GetLength());
                if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    return true;
                } else if (-res == EINTR) {
                    continue;
                } else if (!res) {
                    YDB_LOG_INFO("Connection closed",
                        {"logPrefix", LogPrefix()});
                    PassAway();
                    return false;
                } else if (res < 0) {
                    YDB_LOG_INFO("Connection closed - error",
                        {"logPrefix", LogPrefix()},
                        {"error", strerror(-res)});
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
                            YDB_LOG_ERROR("Wrong message size",
                                {"logPrefix", LogPrefix()},
                                {"size", Request->ExpectedSize});
                            PassAway();
                            return false;
                        }
                        if ((ui64)Request->ExpectedSize > Context->Config.GetMaxMessageSize()) {
                            YDB_LOG_ERROR("Message is big",
                                {"logPrefix", LogPrefix()},
                                {"size", Request->ExpectedSize},
                                {"maxSize", Context->Config.GetMaxMessageSize()});
                            PassAway();
                            return false;
                        }
                        if (static_cast<size_t>(Request->ExpectedSize) < HeaderSize) {
                            YDB_LOG_ERROR("Message is small",
                                {"logPrefix", LogPrefix()},
                                {"size", Request->ExpectedSize});
                            PassAway();
                            return false;
                        }

                        Step = INFLIGHT_CHECK;

                        [[fallthrough]];

                    case INFLIGHT_CHECK:
                        if (!Context->Authenticated() && !PendingRequestsQueue.empty()) {
                            // Allow only one message to be processed at a time for non-authenticated users
                            YDB_LOG_ERROR("DoRead: failed inflight check: there are pending requests and user is not authnicated. Only one paraller request is allowed for a non-authenticated user",
                                {"logPrefix", LogPrefix()},
                                {"pendingRequestsQueue", PendingRequestsQueue.size()});
                            return true;
                        }
                        if (InflightSize + Request->ExpectedSize > Context->Config.GetMaxInflightSize()) {
                            // We limit the size of processed messages so as not to exceed the size of available memory
                            YDB_LOG_ERROR("DoRead: failed inflight check: InflightSize + >",
                                {"logPrefix", LogPrefix()},
                                {"expectedSize", InflightSize + Request->ExpectedSize},
                                {"getMaxInflightSize", Context->Config.GetMaxInflightSize()});
                            return true;
                        }
                        InflightSize += Request->ExpectedSize;
                        Step = MESSAGE_READ;

                        [[fallthrough]];

                    case HEADER_READ:
                        YDB_LOG_TRACE("Start read header",
                            {"logPrefix", LogPrefix()},
                            {"expectedSize", Request->ExpectedSize});

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
                            YDB_LOG_ERROR("CorrelationId already processing",
                                {"logPrefix", LogPrefix()},
                                {"correlationId", Request->CorrelationId});
                            PassAway();
                            return false;
                        }
                        if (!Context->Authenticated() && RequireAuthentication(static_cast<EApiKey>(Request->ApiKey))) {
                            YDB_LOG_ERROR("Unauthenticated request",
                                {"logPrefix", LogPrefix()},
                                {"apiKey", Request->ApiKey});
                            PassAway();
                            return false;
                        }

                        Step = MESSAGE_READ;

                        [[fallthrough]];

                    case MESSAGE_READ:
                        YDB_LOG_TRACE("Start read new message",
                            {"logPrefix", LogPrefix()},
                            {"expectedSize", Request->ExpectedSize});

                        Request->Buffer->Resize(Request->ExpectedSize);
                        Demand = TReadDemand(Request->Buffer->Data() + HeaderSize, Request->ExpectedSize - HeaderSize);

                        Step = MESSAGE_PROCESS;
                        break;

                    case MESSAGE_PROCESS:
                        Request->StartTime = TInstant::Now();
                        if constexpr (DEBUG_ENABLED) {
                            YDB_LOG_DEBUG("Received message",
                                {"logPrefix", LogPrefix()},
                                {"apiKey", Request->ApiKey},
                                {"version", Request->ApiVersion},
                                {"correlationId", Request->CorrelationId},
                                {"data", Hex(Request->Buffer->Begin(), Request->Buffer->End())});
                        } else {
                            YDB_LOG_DEBUG("Received message",
                                {"logPrefix", LogPrefix()},
                                {"apiKey", Request->ApiKey},
                                {"version", Request->ApiVersion},
                                {"correlationId", Request->CorrelationId});
                        }

                        TKafkaReadable readable(*Request->Buffer);
                        readable.SetAllowCompressed(AppData()->FeatureFlags.GetEnableTopicMessagesBatching());

                        try {
                            Request->Message = CreateRequest(Request->ApiKey);

                            Request->Header.Read(readable, RequestHeaderVersion(Request->ApiKey, Request->ApiVersion));
                            Request->Message->Read(readable, Request->ApiVersion);
                        } catch(const yexception& e) {
                            YDB_LOG_ERROR("Error on processing message",
                                {"logPrefix", LogPrefix()},
                                {"apiKey", Request->ApiKey},
                                {"version", Request->ApiVersion},
                                {"correlationId", Request->CorrelationId},
                                {"error", e.what()});
                            PassAway();
                            return false;
                        }

                        Step = SIZE_READ;

                        if (IsSslActive && NKikimr::AppData()->KafkaProxyConfig.GetMtlsEnable() && MtlsAuthStage != MtlsAuthStages::AUTH_SUCCESSFUL) {
                            YDB_LOG_DEBUG("Mtls authentication was not successful",
                                {"logPrefix", LogPrefix()});
                            return false;
                        }

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
                if (IsSslActive && NKikimr::AppData()->KafkaProxyConfig.GetMtlsEnable() && MtlsAuthStage == NO_CERT_YET) {
                    int sslHandshakeResult = Socket->GetSslHandshakeResult();
                    if (sslHandshakeResult != SslHandshakeErrors::ERROR_WANT_READ &&
                        sslHandshakeResult != SslHandshakeErrors::ERROR_WANT_WRITE &&
                        sslHandshakeResult != SslHandshakeErrors::ERROR_NONE) {
                        YDB_LOG_DEBUG("Error in ssl handshake, ssl",
                            {"logPrefix", LogPrefix()},
                            {"errorCode", sslHandshakeResult});
                        PassAway();
                        return;
                    }
                    if (sslHandshakeResult == SslHandshakeErrors::ERROR_NONE) {
                        TSslHelpers::TSslHolder<X509> cert = Socket->GetSslClientCert();
                        if (!cert) {
                            YDB_LOG_ERROR("No cert was received from client during ssl handshake for mTLS authentication",
                                {"logPrefix", LogPrefix()});
                            PassAway();
                            return;
                        }

                        TString clientCert = Socket->GetStringClientCert(cert.get());
                        MtlsAuthStage = PROCESSING_CERT;
                        PollerEventSaved = event;

                        EnsureKafkaSaslAuthActor();
                        Context->AuthenticationStep = EAuthSteps::WAIT_AUTH;
                        Context->SaslMechanism = "MTLS";
                        Context->RequireAuthentication = true;
                        Send(AuthActorId, new TEvKafka::TEvMtlsAuthRequest(clientCert));
                        return;
                    }

                }
                if (!DoRead(ctx)) {
                    return;
                }
            }

            if (event->Get() == InactivityEvent) {
                const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
                if (passed >= InactivityTimeout) {
                    YDB_LOG_DEBUG("Connection closed by inactivity timeout",
                        {"logPrefix", LogPrefix()});
                    return PassAway(); // timeout
                } else {
                    Schedule(InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
                }
            }
        }
        if (event->Get()->Write && !BufferedWriter.Empty()) {
            YDB_LOG_DEBUG("Retrying flush. Buffer queue",
                {"logPrefix", LogPrefix()},
                {"size", BufferedWriter.GetBuffersDeque().size()});
            ssize_t res = BufferedWriter.flush();
            if (res == -EAGAIN || res == -EWOULDBLOCK) {
                YDB_LOG_DEBUG("Socket is busy during retry. Buffer queue Waiting for PollerReady event",
                    {"logPrefix", LogPrefix()},
                    {"size", BufferedWriter.GetBuffersDeque().size()});
                RequestPoller();
                return;
            } else if (res < 0) {
                YDB_LOG_ERROR("Connection closed - error Buffer queue",
                    {"logPrefix", LogPrefix()},
                    {"flushOutput", strerror(-res)},
                    {"size", BufferedWriter.GetBuffersDeque().size()});
                PassAway();
                return;
            } else if (res > 0 && BufferedWriter.Empty()) { // we successfuly retried sending the response
                RetryingWriteToSocket = false;
                auto& request = PendingRequestsQueue.front();
                auto& header = request->Header;
                YDB_LOG_DEBUG("Sent reply (after retry)",
                    {"logPrefix", LogPrefix()},
                    {"apiKey", header.RequestApiKey},
                    {"version", header.RequestApiVersion},
                    {"correlation", header.CorrelationId});
                OnRequestProcessed(request);
                ProcessReplyQueue(ctx);

                if (!CloseConnection && Step == INFLIGHT_CHECK) {
                    DoRead(ctx);
                }
            }
        }

        if (CloseConnection && BufferedWriter.Empty()) {
            YDB_LOG_DEBUG("Connection closed",
                {"logPrefix", LogPrefix()});
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
                YDB_LOG_ERROR("TKafkaConnection: Unexpected",
                    {"logPrefix", LogPrefix()},
                    {"typeName", ev.Get()->GetTypeName()});
        }
    }
};

NActors::IActor* CreateKafkaConnection(const TActorId& listenerActorId,
                                       TIntrusivePtr<TSocketDescriptor> socket,
                                       TNetworkConfig::TSocketAddressType address,
                                       const NKikimrConfig::TKafkaProxyConfig& config,
                                       std::shared_ptr<NKafka::TInet64SecureStreamSocket::TServerMtlsCreds> serverCreds) {
    return new TKafkaConnection(listenerActorId, std::move(socket), std::move(address), config, serverCreds);
}

} // namespace NKafka
