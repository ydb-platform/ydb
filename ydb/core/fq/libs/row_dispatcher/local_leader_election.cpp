#include "leader_election.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/logger/actor.h>

#include <ydb/core/base/path.h>

#include <memory>

#include <ydb/core/grpc_services/local_rpc/local_rpc_bi_streaming.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc_operation.h>
#include <ydb/core/grpc_services/service_coordination.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

namespace {

constexpr TDuration RestartDuration = TDuration::Seconds(3); // Delay before next restart after fatal error
//constexpr TDuration CoordinationSessionTimeout = TDuration::Seconds(30);
constexpr char SemaphoreName[] = "RowDispatcher";
constexpr char DefaultCoordinationNodePath[] = ".metadata/streaming/coordination_node";


using TRpcIn = Ydb::Coordination::SessionRequest;
using TRpcOut = Ydb::Coordination::SessionResponse;
using TLocalRpcCtx = NKikimr::NRpcService::TLocalRpcBiStreamingCtx<TRpcIn, TRpcOut>;

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = TLocalRpcCtx::TRpcEvents::EvEnd,
        EvCreateNodeResult = EvBegin,
        EvCreateSemaphoreResult,
        EvCreateSessionResult,
        EvAcquireSemaphoreResult,
        EvDescribeSemaphoreResult,
        EvSessionStopped,
        EvTimeout,
        EvOnChangedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct  TEvCreateNodeResult :NActors::TEventLocal<TEvCreateNodeResult, EvCreateNodeResult> {
        NYdb::TStatus Status;
        explicit TEvCreateNodeResult(NYdb::TStatus status)
            : Status(std::move(status)) {}
    };

    struct TEvCreateSemaphoreResult : NActors::TEventLocal<TEvCreateSemaphoreResult, EvCreateSemaphoreResult> {
        NYdb::NCoordination::TResult<void> Result;
        explicit TEvCreateSemaphoreResult(const NYdb::NCoordination::TResult<void>& future)
            : Result(std::move(future)) {}
    };
    struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        NYdb::NCoordination::TAsyncSessionResult Result;
        explicit TEvCreateSessionResult(NYdb::NCoordination::TAsyncSessionResult future)
            : Result(std::move(future)) {}
        TEvCreateSessionResult() {}
    };

    struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, EvOnChangedResult> {};

    struct TEvDescribeSemaphoreResult : NActors::TEventLocal<TEvDescribeSemaphoreResult, EvDescribeSemaphoreResult> {
        NYdb::NCoordination::TDescribeSemaphoreResult Result;
        explicit TEvDescribeSemaphoreResult(NYdb::NCoordination::TDescribeSemaphoreResult result)
            : Result(std::move(result)) {}
    };

    struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, EvAcquireSemaphoreResult> {
        NYdb::NCoordination::TResult<bool> Result;
        explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool> future)
            : Result(std::move(future)) {}
    };
    struct TEvSessionStopped : NActors::TEventLocal<TEvSessionStopped, EvSessionStopped> {};
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

struct TActorSystemPtrMixin {
    NKikimr::TDeferredActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr = std::make_shared<NKikimr::TDeferredActorLogBackend::TAtomicActorSystemPtr>(nullptr);
};

class TLocalLeaderElection: public TActorBootstrapped<TLocalLeaderElection>, public TActorSystemPtrMixin {

    enum class EState {
        Init,
        WaitNodeCreated,
        WaitSessionCreated,
        WaitSemaphoreCreated,
        Started
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

public:
    TLocalLeaderElection(
        NActors::TActorId parentId,
        NActors::TActorId coordinatorId,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();
    void PassAway() override;

    [[maybe_unused]] static constexpr char ActorName[] = "YQ_LEADER_EL";

    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateNodeResult::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvSessionStopped::TPtr& ev);
    void Handle(TEvPrivate::TEvRestart::TPtr&);
    void Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvOnChangedResult::TPtr& ev);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvActorAttached::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvReadRequest::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvWriteRequest::TPtr&);
    void Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr&);

    void HandleException(const std::exception& e);

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(TEvPrivate::TEvCreateNodeResult, Handle);
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvCreateSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvAcquireSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvOnChangedResult, Handle);
        hFunc(TEvPrivate::TEvSessionStopped, Handle);
        hFunc(TEvPrivate::TEvRestart, Handle);
        hFunc(TEvPrivate::TEvDescribeSemaphoreResult, Handle);
        cFunc(NActors::TEvents::TSystem::Poison, PassAway);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvActorAttached, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvReadRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvWriteRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvFinishRequest, Handle);,
        ExceptionFunc(std::exception, HandleException)
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
    NYdb::TDriverConfig GetYdbDriverConfig() const;

    void SendStartSession();
    void AddSessionEvent(TRpcIn&& message);
    void SendSessionEvents();
    void SendSessionEvent(TRpcIn&& message, bool success = true);
    void SendSessionEventFail();
    void CloseSession(NYdb::EStatus, const NYql::TIssues& );

    template <typename TRpc, typename TSettings>
    NThreading::TFuture<NKikimr::NRpcService::TLocalRpcOperationResult> DoLocalRpcRequest(typename TRpc::TRequest&& proto, const NYdb::TOperationRequestSettings<TSettings>& settings, NKikimr::NRpcService::TLocalRpcOperationRequestCreator requestCreator) {
        const auto promise = NThreading::NewPromise<NKikimr::NRpcService::TLocalRpcOperationResult>();
        auto* actor = new NKikimr::NRpcService::TOperationRequestExecuter<TRpc, TSettings>(std::move(proto), {
            .ChannelBufferSize = 1000000,// appConfig.GetTableServiceConfig().GetResourceManager().GetChannelBufferSize();,
            .OperationSettings = settings,
            .RequestCreator = std::move(requestCreator),
            .Database = NKikimr::AppData()->TenantName,
            .Token = /*CredentialsProvider ? TMaybe<TString>(CredentialsProvider->GetAuthInfo()) :*/ Nothing(),
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
    : CoordinationNodePath(JoinPath(NKikimr::AppData()->TenantName, DefaultCoordinationNodePath))
    , ParentId(parentId)
    , CoordinatorId(coordinatorId)
    , Metrics(counters) {
}

void TLocalLeaderElection::Bootstrap() {
    Become(&TLocalLeaderElection::StateFunc);
    Y_ABORT_UNLESS(!ActorSystemPtr->load(std::memory_order_relaxed), "Double ActorSystemPtr init");
    ActorSystemPtr->store(TActivationContext::ActorSystem(), std::memory_order_relaxed);

    LogPrefix = "TLeaderElection " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped, local coordinator id " << CoordinatorId.ToString()
         << ", coordination node path " << CoordinationNodePath);
    // if (Config.GetLocalMode()) {
    //     TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(CoordinatorId, 0));
    //     return;
    // }
    ProcessState();
}

void TLocalLeaderElection::ProcessState() {
    switch (State) {
    case EState::Init:
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
}

void TLocalLeaderElection::CreateSemaphore() {
    // Session->CreateSemaphore(SemaphoreName, 1 /* limit */)
    //     .Subscribe(
    //     [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
    //         actorSystem->Send(actorId, new TEvPrivate::TEvCreateSemaphoreResult(future));
    //     }); 
    LOG_ROW_DISPATCHER_DEBUG("Try to create semaphore");
    TRpcIn message;
    auto& inner = *message.mutable_create_semaphore();
    inner.set_req_id(NextReqId++);
    inner.set_name(SemaphoreName);
    inner.set_limit(1);
    AddSessionEvent(std::move(message));
}

void TLocalLeaderElection::AcquireSemaphore() {
    if (PendingAcquire) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Try to acquire semaphore");

    NActorsProto::TActorId protoId;
    ActorIdToProto(CoordinatorId, &protoId);
    TString strActorId;
    if (!protoId.SerializeToString(&strActorId)) {
        Y_ABORT("SerializeToString");
    }
    PendingAcquire = true;
    // Session->AcquireSemaphore(
    //     SemaphoreName,
    //     NYdb::NCoordination::TAcquireSemaphoreSettings().Count(1).Data(strActorId))
    //     .Subscribe(
    //         [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<bool>& future) {
    //             actorSystem->Send(actorId, new TEvPrivate::TEvAcquireSemaphoreResult(future));
    //         });

    TRpcIn message;
    auto& inner = *message.mutable_acquire_semaphore();
    inner.set_req_id(NextReqId++);
    inner.set_name(SemaphoreName);
    inner.set_count(1);
    inner.set_data(strActorId);
    AddSessionEvent(std::move(message));
}

void TLocalLeaderElection::StartSession() {
    LOG_ROW_DISPATCHER_DEBUG("Start session");

    // YdbConnection->CoordinationClient
    //     .StartSession(
    //         CoordinationNodePath, 
    //         NYdb::NCoordination::TSessionSettings()
    //             .Timeout(CoordinationSessionTimeout)
    //             .OnStopped([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()]() {
    //                 actorSystem->Send(actorId, new TEvPrivate::TEvSessionStopped());
    //             }))
    //     .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
    //             actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(future));
    //         });

    const auto& token = /*CredentialsProvider ? std::optional<TString>(CredentialsProvider->GetAuthInfo()) : */std::nullopt;
    auto ctx = MakeIntrusive<TLocalRpcCtx>(ActorContext().ActorSystem(), SelfId(), TLocalRpcCtx::TSettings{
        .Database = NKikimr::AppData()->TenantName,
        .Token = token,
        .PeerName = "localhost/local_topic_rpc_read",
        .RequestType = /*Settings.RequestType_.empty() ?*/ std::nullopt /*: std::optional<TString>(Settings.RequestType_)*/,
      //  .ParentTraceId = TString(Settings.TraceParent_),
      //  .TraceId = TString(Settings.TraceId_),
        .RpcMethodName = "CoordinationService.Session",
        });
    

    auto ev = std::make_unique<NKikimr::NGRpcService::TEvCoordinationSessionRequest>(std::move(ctx), NKikimr::NGRpcService::TRequestAuxSettings{.RequestType = NKikimr::NJaegerTracing::ERequestType::TOPIC_STREAMREAD});


    // if (token) {
    //     ev->SetInternalToken(MakeIntrusive<NACLib::TUserToken>(*token));
    // }

    Send(NKikimr::NGRpcService::CreateGRpcRequestProxyId(), ev.release(), IEventHandle::FlagTrackDelivery);
    

}

void TLocalLeaderElection::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_ROW_DISPATCHER_ERROR("Schema creation error " << ev->Get()->Result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Coordination node successfully created");
    CoordinationNodeCreated = true;
    ProcessState();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvCreateSessionResult::TPtr& /*ev*/) {
    // auto result = ev->Get()->Result.GetValue();
    // if (!result.IsSuccess()) {
    //     LOG_ROW_DISPATCHER_ERROR("CreateSession failed, " << result.GetIssues());
    //     Metrics.Errors->Inc();
    //     ResetState();
    //     return;
    // }
    // Session =  result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("Session successfully created");
    ProcessState();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev) {
    auto result = ev->Get()->Result;
    if (!IsTableCreated(result)) {
        LOG_ROW_DISPATCHER_ERROR("Semaphore creating error " << result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    SemaphoreCreated = true;
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully created");
    ProcessState();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev) {
    auto result = ev->Get()->Result;
    PendingAcquire = false;

    if (!result.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Failed to acquire semaphore, " << result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired " << result.GetResult());
}

void TLocalLeaderElection::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    TActorBootstrapped::PassAway();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvSessionStopped::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionStopped");
    PendingAcquire = false;
    PendingDescribe = false;
    ResetState();
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
    if (PendingDescribe) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Describe semaphore");
    PendingDescribe = true;
    // Session->DescribeSemaphore(
    //     SemaphoreName,
    //     NYdb::NCoordination::TDescribeSemaphoreSettings()
    //         .WatchData()
    //         .WatchOwners()
    //         .IncludeOwners()
    //         .OnChanged([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](bool /* isChanged */) {
    //             actorSystem->Send(actorId, new TEvPrivate::TEvOnChangedResult());
    //         }))
    //     .Subscribe(
    //         [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncDescribeSemaphoreResult& future) {
    //             actorSystem->Send(actorId, new TEvPrivate::TEvDescribeSemaphoreResult(future));
    //         });

    TRpcIn message;
    auto& inner = *message.mutable_describe_semaphore();
    inner.set_req_id(NextReqId++);
    inner.set_name(SemaphoreName);
    inner.set_include_owners(true);
    inner.set_watch_data(true);
    inner.set_watch_owners(true);
    AddSessionEvent(std::move(message));
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvOnChangedResult::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("Semaphore changed");
    PendingDescribe = false;
    ProcessState();
}

void TLocalLeaderElection::Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev) {
    PendingDescribe = false;
    auto result = ev->Get()->Result;
    if (!result.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Semaphore describe fail, " << result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }

    const NYdb::NCoordination::TSemaphoreDescription& description = result.GetResult();
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

void TLocalLeaderElection::HandleException(const std::exception& e) {
    LOG_ROW_DISPATCHER_ERROR("Internal error: exception:" << e.what());
    Metrics.Errors->Inc();
    ResetState();
}

NYdb::TDriverConfig TLocalLeaderElection::GetYdbDriverConfig() const {
    NYdb::TDriverConfig cfg;
    cfg.SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
    cfg.SetLog(std::make_unique<NKikimr::TDeferredActorLogBackend>(ActorSystemPtr, NKikimrServices::EServiceKikimr::YDB_SDK));
    return cfg;
}

void TLocalLeaderElection::CreateNode(const std::string& path,  const NYdb::NCoordination::TCreateNodeSettings& settings) {
    using TCreateNodeRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Coordination::CreateNodeRequest, Ydb::Coordination::CreateNodeResponse>;

    TCreateNodeRequest::TRequest request;
    request.set_path(path);
    //request.set_include_stats(settings.IncludeStats_);
   // request.set_include_location(settings.IncludeLocation_);
 //  Cerr << "CreateNode777 " << path << Endl;

    DoLocalRpcRequest<TCreateNodeRequest, NYdb::NCoordination::TCreateNodeSettings>(std::move(request), settings, &NKikimr::NGRpcService::DoCreateCoordinationNode).Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NThreading::TFuture<NKikimr::NRpcService::TLocalRpcOperationResult>& f) {
        const auto [status, response] = f.GetValue();
      //  Cerr << "TLocalRpcOperationResult22 " << status << "  - "  << status.IsSuccess() << Endl;
        // Ydb::Topic::DescribeTopicResult result;
        // response.UnpackTo(&result);
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateNodeResult(status));

        //return status;
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
   // LOG_ROW_DISPATCHER_DEBUG("TEvActorAttached");
    Y_VALIDATE(!RpcActor, "RpcActor is already set");
    RpcActor = ev->Get()->RpcActor;

    LOG_ROW_DISPATCHER_DEBUG("RpcActor attached: " << RpcActor);
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
   // LOG_ROW_DISPATCHER_DEBUG("Handle TEvWriteRequest");
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
    // const auto status = message.status();
    // if (status != Ydb::StatusIds::SUCCESS) {
    //     NYql::TIssues issues;
    //     IssuesFromMessage(message.issues(), issues);
    //     LOG_ROW_DISPATCHER_DEBUG("Rpc write request, got error " << status << ", reason: " << issues.ToOneLineString());
    //     return CloseSession(status, issues);
    // }

    const auto messageCase = message.response_case();
    LOG_ROW_DISPATCHER_DEBUG("Rpc write request: " << static_cast<i64>(messageCase));

    switch (messageCase) {
        case TRpcOut::kPing:
            LOG_ROW_DISPATCHER_DEBUG("kPing");
            {
                const auto& source = message.ping();
                TRpcIn message;
                message.mutable_pong()->set_opaque(source.opaque());;
                AddSessionEvent(std::move(message));
            }
            break;
        case TRpcOut::kPong:
            LOG_ROW_DISPATCHER_DEBUG("kPong");
            break;
        case TRpcOut::kFailure:
            LOG_ROW_DISPATCHER_DEBUG("kFailure");
            {
                auto failure = message.failure();
                const auto status = failure.status();
                if (status != Ydb::StatusIds::SUCCESS) {
                    NYql::TIssues issues;
                    IssuesFromMessage(failure.issues(), issues);
                    LOG_ROW_DISPATCHER_DEBUG("Rpc write request, got error " << status << ", reason: " << issues.ToOneLineString());
                }
            }
            break;
        case TRpcOut::kSessionStarted:
            LOG_ROW_DISPATCHER_DEBUG("kSessionStarted");
            {
            const auto& source = message.session_started();
            SessionId = source.session_id();
            Send(SelfId(), new TEvPrivate::TEvCreateSessionResult());
            }
            break;
        case TRpcOut::kSessionStopped:
            LOG_ROW_DISPATCHER_DEBUG("kSessionStopped");
            break;
        case TRpcOut::kAcquireSemaphorePending:
            LOG_ROW_DISPATCHER_DEBUG("kAcquireSemaphorePending");
            break;
        case TRpcOut::kAcquireSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("kAcquireSemaphoreResult");
            {
            const auto& source = message.acquire_semaphore_result();
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
            auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));
            Send(SelfId(), new TEvPrivate::TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool>(status, source.acquired())));
            }
            break;
        case TRpcOut::kReleaseSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("ReleaseSemaphoreResult");
            break;
        case TRpcOut::kDescribeSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("DescribeSemaphoreResult");
{
            const auto& source = message.describe_semaphore_result();
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
            auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));
            
            Send(SelfId(), new TEvPrivate::TEvDescribeSemaphoreResult(NYdb::NCoordination::TDescribeSemaphoreResult(status, source.semaphore_description())));
}
            break;
        case TRpcOut::kDescribeSemaphoreChanged:
            LOG_ROW_DISPATCHER_DEBUG("DescribeSemaphoreChanged");
            Send(SelfId(), new TEvPrivate::TEvOnChangedResult());
            break;
        case TRpcOut::kCreateSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("CreateSemaphoreResult");
            {
            const auto& source = message.create_semaphore_result();
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::IssuesFromMessage(source.issues(), issues);
            auto status = NYdb::TStatus(static_cast<NYdb::EStatus>(source.status()), std::move(issues));

            Send(SelfId(), new TEvPrivate::TEvCreateSemaphoreResult(NYdb::NCoordination::TResult<void>(status)));
             }
            break;
        case TRpcOut::kUpdateSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("UpdateSemaphoreResult");
            break;
        case TRpcOut::kDeleteSemaphoreResult:
            LOG_ROW_DISPATCHER_DEBUG("DeleteSemaphoreResult");
            break;
        default:
            LOG_ROW_DISPATCHER_DEBUG("Unknown message case");
    }
}
void TLocalLeaderElection::Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvFinishRequest");
}

void TLocalLeaderElection::SendStartSession() {
    // TRequest req;
    // auto* start = req.mutable_session_start();
    // start->set_seq_no(seqNo);
    // start->set_session_id(sessionId);
    // start->set_path(TStringType{Path_});
    // start->set_timeout_millis(Settings_.Timeout_ != TDuration::Max() ?
    //     Settings_.Timeout_.MilliSeconds() : Max<uint64_t>());
    // start->set_description(TStringType{Settings_.Description_});
    // start->set_protection_key(TStringType{ProtectionKey_});
    // processor->Write(std::move(req));

    LOG_ROW_DISPATCHER_DEBUG("Sending start message");

    TRpcIn message;

    auto& start = *message.mutable_session_start();
    start.set_seq_no(0);
    start.set_path(CoordinationNodePath);

    AddSessionEvent(std::move(message));
}

void TLocalLeaderElection::AddSessionEvent(TRpcIn&& message) {
    // if (SessionClosed) {
    //     LOG_D("Session already closed, skip session event");
    //     return;
    // }

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

void TLocalLeaderElection::CloseSession(NYdb::EStatus, const NYql::TIssues& ) {

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
