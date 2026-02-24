#include "local_leader_election.h"
#include <memory>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/core/grpc_services/local_rpc/local_rpc_bi_streaming.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc_operation.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_coordination.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/logger/actor.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

namespace {

constexpr TDuration RestartDuration = TDuration::Seconds(3); // Delay before next restart after fatal error

constexpr char SemaphoreName[] = "RowDispatcher";
constexpr char DefaultCoordinationNodePath[] = ".metadata/streaming/coordination_node";
constexpr TDuration SessionTimeout = TDuration::Seconds(1);

using TRpcIn = Ydb::Coordination::SessionRequest;
using TRpcOut = Ydb::Coordination::SessionResponse;
using TLocalRpcCtx = NKikimr::NRpcService::TLocalRpcBiStreamingCtx<TRpcIn, TRpcOut>;

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = TLocalRpcCtx::TRpcEvents::EvEnd,
        EvCreateNodeResult = EvBegin,
        EvSelfPing,
        EvSessionStopped,
        EvTimeout,
        EvOnChangedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct  TEvCreateNodeResult : NActors::TEventLocal<TEvCreateNodeResult, EvCreateNodeResult> {
        NYdb::TStatus Status;
        explicit TEvCreateNodeResult(NYdb::TStatus status)
            : Status(std::move(status)) {}
    };

    struct TEvSelfPing : NActors::TEventLocal<TEvSelfPing, EvSelfPing> {};
    struct TEvRestart : NActors::TEventLocal<TEvRestart, EvTimeout> {};
};

////////////////////////////////////////////////////////////////////////////////

struct TLeaderElectionMetrics {
    explicit TLeaderElectionMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        Errors = Counters->GetCounter("LeaderElectionErrors", true);
        LeaderChanged = Counters->GetCounter("LeaderChanged", true);
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr LeaderChanged;
};


class TLocalLeaderElection: public TActorBootstrapped<TLocalLeaderElection>, public IActorExceptionHandler {

    enum class EState {
        Init,
        WaitNodeCreated,
        WaitSessionCreated,
        WaitSemaphoreCreated,
        Started
    };

    struct TOperation {
        TInstant SendTimestamp = TInstant::Now();
    };


    TString CoordinationNodePath;
    TActorId ParentId;
    TActorId CoordinatorId;
    TString LogPrefix;
    EState State = EState::Init;
    bool CoordinationNodeCreated = false;
    bool SemaphoreCreated = false;
    bool RestartScheduled = false;
    bool PendingDescribe = false;
    bool PendingDescribeChanged = false;
    bool PendingAcquire = false;

    TMaybe<TActorId> LeaderActorId;

    struct NodeInfo {
        bool Connected = false;
    };
    std::map<ui32, NodeInfo> RowDispatchersByNode;
    TLeaderElectionMetrics Metrics;

    TActorId RpcActor;
    std::queue<TRpcIn> RpcResponses;
    bool SessionClosed = false;
    ui64 PendingRpcResponses = 0;

    uint64_t SessionSeqNo = 0;
    uint64_t SessionId = 0;
    uint64_t NextReqId = 1;

    TInstant SessionLastKnownGoodTimestamp;
    std::unordered_map<uint64_t, std::unique_ptr<TOperation>> SentRequests;

public:
    TLocalLeaderElection(
        NActors::TActorId parentId,
        NActors::TActorId coordinatorId,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();
    void PassAway() override;

    [[maybe_unused]] static constexpr char ActorName[] = "YQ_LEADER_EL";

    void Handle(TEvPrivate::TEvCreateNodeResult::TPtr& ev);
    void Handle(TEvPrivate::TEvSelfPing::TPtr& ev);
    void Handle(TEvPrivate::TEvRestart::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvActorAttached::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvReadRequest::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvWriteRequest::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr&);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);

    bool OnUnhandledException(const std::exception& e) override;

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCreateNodeResult, Handle);
        hFunc(TEvPrivate::TEvSelfPing, Handle);
        hFunc(TEvPrivate::TEvRestart, Handle);
        cFunc(NActors::TEvents::TSystem::Poison, PassAway);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvActorAttached, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvReadRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvWriteRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvFinishRequest, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

private:
    void CreateSemaphore();
    void AcquireSemaphore();
    void DebugPrint();
    void StartSession();
    void DescribeSemaphore();
    void ProcessState();
    void ResetState();
    void SetTimeout();
    void SendStartSession();
    void AddSessionEvent(TRpcIn&& message);
    void SendSessionEvents();
    void SendSessionEvent(TRpcIn&& message, bool success = true);
    void SendSessionEventFail();
    void CloseSession(NYdb::EStatus, const NYql::TIssues&);
    void CloseSession(const grpc::Status& status, const TString& message = "");

    void ProcessPing(const TRpcOut& message);
    void ProcessPong(const TRpcOut& message);
    void ProcessFailure(const TRpcOut& message);
    void ProcessSessionStarted(const TRpcOut& message);
    void ProcessSessionStopped(const TRpcOut& message);
    void ProcessCreateSemaphoreResult(const TRpcOut& message);
    void ProcessAcquireSemaphoreResult(const TRpcOut& message);
    void ProcessDescribeSemaphoreChanged(const TRpcOut& message);
    void ProcessReleaseSemaphoreResult(const TRpcOut& message);
    void ProcessAcquireSemaphorePending(const TRpcOut& message);
    void ProcessDescribeSemaphoreResult(const TRpcOut& message);
    void ProcessUpdateSemaphoreResult(const TRpcOut& message);
    void ProcessDeleteSemaphoreResult(const TRpcOut& message);

    NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues);
    void UpdateLastKnownGoodTimestampLocked(TInstant timestamp);
    TOperation* FindSentRequest(uint64_t reqId) const;

    template <typename TRpc, typename TSettings>
    NThreading::TFuture<NKikimr::NRpcService::TLocalRpcOperationResult> DoLocalRpcRequest(typename TRpc::TRequest&& proto, const NYdb::TOperationRequestSettings<TSettings>& settings, NKikimr::NRpcService::TLocalRpcOperationRequestCreator requestCreator) {
        const auto promise = NThreading::NewPromise<NKikimr::NRpcService::TLocalRpcOperationResult>();
        auto token = NACLib::TSystemUsers::Metadata().SerializeAsString();
        auto* actor = new NKikimr::NRpcService::TOperationRequestExecuter<TRpc, TSettings>(std::move(proto), {
            .ChannelBufferSize = 16000,
            .OperationSettings = settings,
            .RequestCreator = std::move(requestCreator),
            .Database = NKikimr::AppData()->TenantName,
            .Token = token,
            .Promise = promise,
            .OperationName = "local_coordination_rpc_operation",
        });
        Register(actor, TMailboxType::HTSwap, TActivationContext::ActorSystem()->AppData<NKikimr::TAppData>()->UserPoolId);
        return promise.GetFuture();
    }

    void CreateNode(const std::string& path, const NYdb::NCoordination::TCreateNodeSettings& settings = NYdb::NCoordination::TCreateNodeSettings());
};

TLocalLeaderElection::TLocalLeaderElection(
    NActors::TActorId parentId,
    NActors::TActorId coordinatorId,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : ParentId(parentId)
    , CoordinatorId(coordinatorId)
    , Metrics(counters) {
}

void TLocalLeaderElection::Bootstrap() {
    Become(&TLocalLeaderElection::StateFunc);

    CoordinationNodePath = JoinPath(NKikimr::AppData()->TenantName, DefaultCoordinationNodePath);

    LogPrefix = "TLeaderElection " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped, local coordinator id " << CoordinatorId.ToString()
         << ", coordination node path " << CoordinationNodePath);
    ProcessState();
}

void TLocalLeaderElection::ProcessState() {
    switch (State) {
    case EState::Init:
        if (RestartScheduled) {
            return;
        }
        CreateNode(CoordinationNodePath);
        State = EState::WaitNodeCreated;
        [[fallthrough]];
    case EState::WaitNodeCreated:
        if (!CoordinationNodeCreated) {
            return;
        }
        if (!SessionId) {
            StartSession();
        }
        State = EState::WaitSessionCreated;
        [[fallthrough]];
    case EState::WaitSessionCreated:
        if (!SessionId) {
            return;
        }
        if (!SemaphoreCreated) {
            CreateSemaphore();
        }
        State = EState::WaitSemaphoreCreated;
        [[fallthrough]];
    case EState::WaitSemaphoreCreated:
        if (!SemaphoreCreated) {
            return;
        }
        State = EState::Started;
        [[fallthrough]];
    case EState::Started:
        AcquireSemaphore();
        DescribeSemaphore();
        break;
    }
}

void TLocalLeaderElection::ResetState() {
    State = EState::Init;
    SetTimeout();
    SessionId = 0;
    while (PendingRpcResponses) {  
        SendSessionEventFail();
    }
    PendingRpcResponses = 0;
    RpcResponses = {};
    PendingAcquire = false;
    SentRequests.clear();
    RpcActor = {};
}

void TLocalLeaderElection::CreateSemaphore() {
    LOG_ROW_DISPATCHER_DEBUG("Try to create semaphore");
    TRpcIn message;
    auto& inner = *message.mutable_create_semaphore();
    uint64_t reqId = NextReqId++;
    inner.set_req_id(reqId);
    inner.set_name(SemaphoreName);
    inner.set_limit(1);
    AddSessionEvent(std::move(message));
    SentRequests[reqId] = std::make_unique<TOperation>();
}

void TLocalLeaderElection::AcquireSemaphore() {
    if (PendingAcquire) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Try to acquire semaphore");

    NActorsProto::TActorId protoId;
    ActorIdToProto(CoordinatorId, &protoId);
    TString strActorId;
    Y_ABORT_UNLESS(protoId.SerializeToString(&strActorId));
    PendingAcquire = true;

    TRpcIn message;
    auto& inner = *message.mutable_acquire_semaphore();
    uint64_t reqId = NextReqId++;
    inner.set_req_id(reqId);
    inner.set_name(SemaphoreName);
    inner.set_count(1);
    inner.set_data(strActorId);
    inner.set_ephemeral(false);
    inner.set_timeout_millis(SessionTimeout.MilliSeconds());
    AddSessionEvent(std::move(message));
    SentRequests[reqId] = std::make_unique<TOperation>();
}

void TLocalLeaderElection::StartSession() {
    LOG_ROW_DISPATCHER_DEBUG("Start session");
    auto token = NACLib::TUserToken(BUILTIN_ACL_METADATA, {}).SerializeAsString();
    auto ctx = MakeIntrusive<TLocalRpcCtx>(ActorContext().ActorSystem(), SelfId(), TLocalRpcCtx::TSettings{
        .Database = NKikimr::AppData()->TenantName,
        .Token = token,
        .PeerName = "localhost/local_coordination_rpc_read",
        .RequestType = std::nullopt,
        .RpcMethodName = "CoordinationService.Session",
        });
    
    auto ev = std::make_unique<NKikimr::NGRpcService::TEvCoordinationSessionRequest>(std::move(ctx), NKikimr::NGRpcService::TRequestAuxSettings{});
    if (token) {
        ev->SetInternalToken(MakeIntrusive<NACLib::TUserToken>(token));
    }
    Send(NKikimr::NGRpcService::CreateGRpcRequestProxyId(), ev.release(), IEventHandle::FlagTrackDelivery);
}

void TLocalLeaderElection::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    TActorBootstrapped::PassAway();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvSelfPing::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSelfPing");
    if (SessionClosed) {
        return;
    }

    TInstant nextTimerTimestamp;
    auto now = TInstant::Now();
    auto half = SessionLastKnownGoodTimestamp + SessionTimeout / 2;
    if (now < half) {
        nextTimerTimestamp = SessionLastKnownGoodTimestamp + SessionTimeout * (2.0 / 3.0);
    } else {
        TRpcIn message;
        auto& inner = *message.mutable_ping();
        uint64_t reqId = NextReqId++;
        inner.set_opaque(reqId);
        AddSessionEvent(std::move(message));
        SentRequests[reqId] = std::make_unique<TOperation>();

        auto expectedSessionDeadline = SessionLastKnownGoodTimestamp + SessionTimeout;
        auto minimalWaitDeadline = now + SessionTimeout / 4;
        nextTimerTimestamp = Max(expectedSessionDeadline, minimalWaitDeadline);
    }
    Schedule(nextTimerTimestamp - TInstant::Now(), new TEvPrivate::TEvSelfPing());
}

void TLocalLeaderElection::SetTimeout() {
    if (RestartScheduled) {
        return;
    }
    RestartScheduled = true;
    Schedule(RestartDuration, new TEvPrivate::TEvRestart());
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvRestart::TPtr&) {
    RestartScheduled = false;
    LOG_ROW_DISPATCHER_DEBUG("TEvRestart");
    ProcessState(); 
}

void TLocalLeaderElection::DescribeSemaphore() {
    if (PendingDescribe || PendingDescribeChanged) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Describe semaphore");
    PendingDescribe = true;
    PendingDescribeChanged = true;

    TRpcIn message;
    auto& inner = *message.mutable_describe_semaphore();
    uint64_t reqId = NextReqId++;
    inner.set_req_id(reqId);
    inner.set_name(SemaphoreName);
    inner.set_include_owners(true);
    inner.set_watch_data(true);
    inner.set_watch_owners(true);
    AddSessionEvent(std::move(message));
    SentRequests[reqId] = std::make_unique<TOperation>();
}

bool TLocalLeaderElection::OnUnhandledException(const std::exception& e) {
    LOG_ROW_DISPATCHER_ERROR("Internal error: exception:" << e.what());
    Metrics.Errors->Inc();
    ResetState();
    return true;
}

void TLocalLeaderElection::CreateNode(const std::string& path,  const NYdb::NCoordination::TCreateNodeSettings& settings) {
    using TCreateNodeRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Coordination::CreateNodeRequest, Ydb::Coordination::CreateNodeResponse>;

    if (CoordinationNodeCreated) {
        return;
    }
    TCreateNodeRequest::TRequest request;
    request.set_path(path);

    DoLocalRpcRequest<TCreateNodeRequest, NYdb::NCoordination::TCreateNodeSettings>(std::move(request), settings, &NKikimr::NGRpcService::DoCreateCoordinationNode).Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NThreading::TFuture<NKikimr::NRpcService::TLocalRpcOperationResult>& f) {
        const auto [status, response] = f.GetValue();
        actorSystem->Send(actorId, new TEvPrivate::TEvCreateNodeResult(status));
    });
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvCreateNodeResult::TPtr& ev) {
    const auto& status = ev->Get()->Status;
    if (!status.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Coordination node creation error: " << status.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Coordination node successfully created");
    CoordinationNodeCreated = true;
    ProcessState();
}

void TLocalLeaderElection::Handle(TLocalRpcCtx::TRpcEvents::TEvActorAttached::TPtr& ev) {
    Y_VALIDATE(!RpcActor, "RpcActor is already set");
    RpcActor = ev->Get()->RpcActor;
    SessionClosed = false;

    LOG_ROW_DISPATCHER_DEBUG("RpcActor attached: " << RpcActor);
    RpcResponses = {};
    SendStartSession();
}

void TLocalLeaderElection::Handle(TLocalRpcCtx::TRpcEvents::TEvReadRequest::TPtr&) {
    PendingRpcResponses++;

    if (SessionClosed) {
        LOG_ROW_DISPATCHER_DEBUG("Rpc read request skipped, session is closed");
        SendSessionEventFail();
        return;
    }

    LOG_ROW_DISPATCHER_DEBUG("Rpc read request");
    SendSessionEvents();
}

void TLocalLeaderElection::Handle(TLocalRpcCtx::TRpcEvents::TEvWriteRequest::TPtr& ev) {
    Y_VALIDATE(RpcActor, "RpcActor is not set before write request");
    auto response = std::make_unique<TLocalRpcCtx::TEvWriteFinished>();

    if (SessionClosed) {
        LOG_ROW_DISPATCHER_DEBUG("Rpc write request skipped, session is closed");
        response->Success = false;
        Send(RpcActor, response.release());
        return;
    }

    response->Success = true;
    Send(RpcActor, response.release());

    auto& message = ev->Get()->Message;
    const auto messageCase = message.response_case();
    LOG_ROW_DISPATCHER_DEBUG("Rpc write request: " << static_cast<i64>(messageCase));

    switch (messageCase) {
        case TRpcOut::kPing:
            ProcessPing(message);
            break;
        case TRpcOut::kPong:
            ProcessPong(message);
            break;
        case TRpcOut::kFailure:
            ProcessFailure(message);
            break;
        case TRpcOut::kSessionStarted:
            ProcessSessionStarted(message);
            break;
        case TRpcOut::kSessionStopped:
            ProcessSessionStopped(message);
            break;
        case TRpcOut::kAcquireSemaphorePending:
            ProcessAcquireSemaphorePending(message);
            break;
        case TRpcOut::kAcquireSemaphoreResult:
            ProcessAcquireSemaphoreResult(message);
            break;
        case TRpcOut::kReleaseSemaphoreResult:
            ProcessReleaseSemaphoreResult(message);
            break;
        case TRpcOut::kDescribeSemaphoreResult:
            ProcessDescribeSemaphoreResult(message);
            break;
        case TRpcOut::kDescribeSemaphoreChanged:
            ProcessDescribeSemaphoreChanged(message);
            break;
        case TRpcOut::kCreateSemaphoreResult:
            ProcessCreateSemaphoreResult(message);
            break;
        case TRpcOut::kUpdateSemaphoreResult:
            ProcessUpdateSemaphoreResult(message);
            break;
        case TRpcOut::kDeleteSemaphoreResult:
            ProcessDeleteSemaphoreResult(message);
            break;
        default:
            LOG_ROW_DISPATCHER_DEBUG("Unknown message case");
    }
    ProcessState();
}

void TLocalLeaderElection::Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr& ev) {
    const auto& status = ev->Get()->Status;
    if (!status.ok()) {
        LOG_ROW_DISPATCHER_ERROR("Rpc session finished with error status code: " << static_cast<ui64>(status.error_code()) << ", message: " << status.error_message());
    } else {
        LOG_ROW_DISPATCHER_INFO("Rpc session successfully finished");
    }

    CloseSession(status, "Read session closed");
    ResetState();
}

void TLocalLeaderElection::SendStartSession() {
    Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
    LOG_ROW_DISPATCHER_DEBUG("Sending start message");

    TRpcIn message;
    auto& start = *message.mutable_session_start();
    start.set_seq_no(0);
    start.set_path(CoordinationNodePath);
    start.set_timeout_millis(SessionTimeout.MilliSeconds());
    AddSessionEvent(std::move(message));
}

void TLocalLeaderElection::AddSessionEvent(TRpcIn&& message) {
    if (SessionClosed) {
        LOG_ROW_DISPATCHER_DEBUG("Session already closed, skip session event");
        return;
    }

    RpcResponses.push(message);
    LOG_ROW_DISPATCHER_DEBUG("Added session event: " << static_cast<i64>(message.request_case()));

    if (RpcActor) {
        SendSessionEvents();
    }
}

void TLocalLeaderElection::SendSessionEvents() {
    Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
    LOG_ROW_DISPATCHER_DEBUG("Going to send session events, PendingRpcResponses: " << PendingRpcResponses << ", RpcResponses #" << RpcResponses.size());

    while (PendingRpcResponses > 0 && !RpcResponses.empty()) {
        SendSessionEvent(std::move(RpcResponses.front()));
        RpcResponses.pop();
    }
}

void TLocalLeaderElection::SendSessionEvent(TRpcIn&& message, bool success) {
    LOG_ROW_DISPATCHER_DEBUG("Sending session event: " << static_cast<i64>(message.request_case()) << ", success: " << success);
    Y_VALIDATE(PendingRpcResponses > 0, "Rpc read is not expected");
    PendingRpcResponses--;

    auto ev = std::make_unique<TLocalRpcCtx::TEvReadFinished>();
    ev->Success = success;
    ev->Record = std::move(message);

    Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
    Send(RpcActor, ev.release());
}

void TLocalLeaderElection::SendSessionEventFail() {
    SendSessionEvent({}, /* success */ false);
}

void TLocalLeaderElection::CloseSession(NYdb::EStatus status, const NYql::TIssues& issues) {
    const bool success = status ==  NYdb::EStatus::SUCCESS;
    if (!success) {
        Metrics.Errors->Inc();
        LOG_ROW_DISPATCHER_ERROR("Closing session with status " << status << " and issues: " << issues.ToOneLineString());
    } else {
        LOG_ROW_DISPATCHER_INFO("Closing session with success status");
    }

    if (SessionClosed) {
        LOG_ROW_DISPATCHER_WARN("Session already closed, but got status " << status << " and issues: " << issues.ToOneLineString());
        return;
    }
    SessionClosed = true;

    // Close session on server side
    while (PendingRpcResponses) {
        SendSessionEventFail();
    }
    PendingRpcResponses = 0;
    Send(RpcActor, new TLocalRpcCtx::TEvNotifiedWhenDone(success));
    RpcActor = {};
}

NYql::TIssues TLocalLeaderElection::AddRootIssue(const TString& message, const NYql::TIssues& issues) {
    if (!issues) {
        return {};
    }

    NYql::TIssue rootIssue(message);
    for (const auto& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    return {rootIssue};
}

void TLocalLeaderElection::CloseSession(const grpc::Status& status, const TString& message) {
    NYql::TIssues issues;
    if (const auto& errorMessage = status.error_message(); !errorMessage.empty()) {
        issues.AddIssue(errorMessage);
    }
    if (message) {
        issues = AddRootIssue(message, issues);
    }

    switch (status.error_code()) {
        case grpc::OK:
            return CloseSession(NYdb::EStatus::SUCCESS, issues);
        case grpc::CANCELLED:
            return CloseSession(NYdb::EStatus::CANCELLED, issues);
        case grpc::UNKNOWN:
            return CloseSession(NYdb::EStatus::UNDETERMINED, issues);
        case grpc::DEADLINE_EXCEEDED:
            return CloseSession(NYdb::EStatus::TIMEOUT, issues);
        case grpc::NOT_FOUND:
            return CloseSession(NYdb::EStatus::NOT_FOUND, issues);
        case grpc::ALREADY_EXISTS:
            return CloseSession(NYdb::EStatus::ALREADY_EXISTS, issues);
        case grpc::RESOURCE_EXHAUSTED:
            return CloseSession(NYdb::EStatus::OVERLOADED, issues);
        case grpc::ABORTED:
            return CloseSession(NYdb::EStatus::ABORTED, issues);
        case grpc::OUT_OF_RANGE:
            return CloseSession(NYdb::EStatus::CLIENT_OUT_OF_RANGE, issues);
        case grpc::UNIMPLEMENTED:
            return CloseSession(NYdb::EStatus::UNSUPPORTED, issues);
        case grpc::UNAVAILABLE:
            return CloseSession(NYdb::EStatus::UNAVAILABLE, issues);
        case grpc::INVALID_ARGUMENT:
        case grpc::FAILED_PRECONDITION:
            return CloseSession(NYdb::EStatus::PRECONDITION_FAILED, issues);
        case grpc::UNAUTHENTICATED:
        case grpc::PERMISSION_DENIED:
            return CloseSession(NYdb::EStatus::UNAUTHORIZED, issues);
        default:
            return CloseSession(NYdb::EStatus::INTERNAL_ERROR, issues);
    }
}

void TLocalLeaderElection::ProcessPing(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received Ping, send Pong");
    const auto& source = message.ping();
    TRpcIn in;
    in.mutable_pong()->set_opaque(source.opaque());
    AddSessionEvent(std::move(in));
}

void TLocalLeaderElection::ProcessPong(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received Pong");
    const auto& source = message.pong();
    const uint64_t reqId = source.opaque();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
}

void TLocalLeaderElection::ProcessFailure(const TRpcOut& message) {
    auto failure = message.failure();
    const auto status = failure.status();

    NYql::TIssues issues;
    IssuesFromMessage(failure.issues(), issues);
    LOG_ROW_DISPATCHER_DEBUG("Received Failure, status: " << status << ", issues: " << issues.ToOneLineString());

    CloseSession(static_cast<NYdb::EStatus>(status), issues);
    ResetState();
}

void TLocalLeaderElection::ProcessSessionStarted(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received SessionStarted, session successfully created");
    const auto& source = message.session_started();
    SessionId = source.session_id();

    Schedule(SessionTimeout * (2.0 / 3.0), new TEvPrivate::TEvSelfPing());
    ProcessState();
}

void TLocalLeaderElection::ProcessSessionStopped(const TRpcOut& /*message*/) {
    LOG_ROW_DISPATCHER_DEBUG("Received SessionStopped");
    ResetState();
}

void TLocalLeaderElection::ProcessCreateSemaphoreResult(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received CreateSemaphoreResult");
    const auto& source = message.create_semaphore_result();
    NYdb::NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
    
    auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));
    LOG_ROW_DISPATCHER_INFO("Semaphore creating status: " << status);
    if (!IsTableCreated(status)) {
        LOG_ROW_DISPATCHER_ERROR("Semaphore creating error " << issues.ToOneLineString());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    const uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
    SemaphoreCreated = true;
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully created");
    ProcessState();
}

void TLocalLeaderElection::ProcessAcquireSemaphoreResult(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received AcquireSemaphoreResult");
    const auto& source = message.acquire_semaphore_result();
    NYdb::NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
    auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));

    PendingAcquire = false;

    if (!status.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Failed to acquire semaphore, " << issues.ToOneLineString());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
    if (source.acquired()) {
        LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired");
    } else {
        LOG_ROW_DISPATCHER_DEBUG("Semaphore acquire timed out");
    }
}

void TLocalLeaderElection::ProcessDescribeSemaphoreResult(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received DescribeSemaphoreResult");
    const auto& source = message.describe_semaphore_result();
    NYdb::NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
    auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));

    PendingDescribe = false;
    if (!status.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Semaphore describe fail, " << status.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    const uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    if (!source.watch_added()) {
        SentRequests.erase(reqId);
    }

    const NYdb::NCoordination::TSemaphoreDescription& description = source.semaphore_description();
    Y_ABORT_UNLESS(description.GetOwners().size() <= 1, "To many owners");
    if (description.GetOwners().empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Empty owners");
        // Wait OnChanged.
        return;
    }
    const auto& session = description.GetOwners()[0];
    auto data = TString{session.GetData()};
    auto generation = session.GetOrderId();
    NActorsProto::TActorId protoId;
    if (!protoId.ParseFromString(data)) {
        Y_ABORT("ParseFromString");
    }

    NActors::TActorId id = ActorIdFromProto(protoId);
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully described: coordinator id " << id << " generation " << generation);
    if (!LeaderActorId || (*LeaderActorId != id)) {
        LOG_ROW_DISPATCHER_INFO("Send TEvCoordinatorChanged to " << ParentId << ", new coordinator id " << id << ", previous coordinator id " << LeaderActorId.GetOrElse(TActorId()));
        TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(id, generation));
        Metrics.LeaderChanged->Inc();
    }
    LeaderActorId = id;
}

void TLocalLeaderElection::ProcessDescribeSemaphoreChanged(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received DescribeSemaphoreChanged");
    PendingDescribeChanged = false;

    const auto& source = message.describe_semaphore_changed();
    const uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
}

void TLocalLeaderElection::ProcessReleaseSemaphoreResult(const TRpcOut& /*message*/) {    
    LOG_ROW_DISPATCHER_DEBUG("Received ReleaseSemaphoreResult");
}

void TLocalLeaderElection::ProcessAcquireSemaphorePending(const TRpcOut& /*message*/) {
    LOG_ROW_DISPATCHER_DEBUG("Received AcquireSemaphorePending");
}

void TLocalLeaderElection::ProcessUpdateSemaphoreResult(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received UpdateSemaphoreResult");

    const auto& source = message.update_semaphore_result();
    const uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
}

void TLocalLeaderElection::ProcessDeleteSemaphoreResult(const TRpcOut& message) {
    LOG_ROW_DISPATCHER_DEBUG("Received DeleteSemaphoreResult");
    const auto& source = message.delete_semaphore_result();
    const uint64_t reqId = source.req_id();
    auto* op = FindSentRequest(reqId);
    if (op) {
        UpdateLastKnownGoodTimestampLocked(op->SendTimestamp);
    }
    SentRequests.erase(reqId);
}

void TLocalLeaderElection::UpdateLastKnownGoodTimestampLocked(TInstant timestamp) {
    SessionLastKnownGoodTimestamp = Max(SessionLastKnownGoodTimestamp, timestamp);
    LOG_ROW_DISPATCHER_TRACE("UpdateLastKnownGoodTimestampLocked timestamp " << timestamp << " new " << SessionLastKnownGoodTimestamp);
}

TLocalLeaderElection::TOperation* TLocalLeaderElection::FindSentRequest(uint64_t reqId) const {
    auto it = SentRequests.find(reqId);
    if (it == SentRequests.end()) {
        return nullptr;
    }
    return it->second.get();
}

void TLocalLeaderElection::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    const auto sourceType = ev->Get()->SourceType;
    const auto reason = ev->Get()->Reason;
    Y_VALIDATE(sourceType == NKikimr::NGRpcService::TEvCoordinationSessionRequest::EventType, "Unexpected undelivered event: " << sourceType << ", reason: " << reason);

    LOG_ROW_DISPATCHER_ERROR("Coordination service is unavailable, reason: " << reason);
    CloseSession(NYdb::EStatus::INTERNAL_ERROR, {NYql::TIssue("Coordination service is unavailable, please contact internal support")});
    ResetState();
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLocalLeaderElection(
    NActors::TActorId rowDispatcherId,
    NActors::TActorId coordinatorId,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return std::unique_ptr<NActors::IActor>(new TLocalLeaderElection(rowDispatcherId, coordinatorId, counters));
}

} // namespace NFq
