#include "coordinator.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;
using NYql::TIssues;

namespace {

const ui64 TimeoutDurationSec = 3;
const TString SemaphoreName = "RowDispatcher";

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSemaphoreResult = EvBegin,
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
    struct TEvCreateSemaphoreResult : NActors::TEventLocal<TEvCreateSemaphoreResult, EvCreateSemaphoreResult> {
        NYdb::NCoordination::TAsyncResult<void> Result;
        explicit TEvCreateSemaphoreResult(const NYdb::NCoordination::TAsyncResult<void>& future)
            : Result(std::move(future)) {}
    };
    struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        NYdb::NCoordination::TAsyncSessionResult Result;
        explicit TEvCreateSessionResult(NYdb::NCoordination::TAsyncSessionResult future)
            : Result(std::move(future)) {}
    };

    struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, EvOnChangedResult> {
        bool Result;
        explicit TEvOnChangedResult(bool result)
            : Result(result) {}
    };

    struct TEvDescribeSemaphoreResult : NActors::TEventLocal<TEvDescribeSemaphoreResult, EvDescribeSemaphoreResult> {
        NYdb::NCoordination::TAsyncDescribeSemaphoreResult Result;
        explicit TEvDescribeSemaphoreResult(NYdb::NCoordination::TAsyncDescribeSemaphoreResult future)
            : Result(std::move(future)) {}
    };

    struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, EvAcquireSemaphoreResult> {
        NYdb::NCoordination::TAsyncResult<bool> Result;
        explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TAsyncResult<bool> future)
            : Result(std::move(future)) {}
    };
    struct TEvSessionStopped : NActors::TEventLocal<TEvSessionStopped, EvSessionStopped> {};
    struct TEvTimeout : NActors::TEventLocal<TEvTimeout, EvTimeout> {};
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

class TLeaderElection: public TActorBootstrapped<TLeaderElection> {

    enum class EState {
        Init,
        WaitNodeCreated,
        WaitSessionCreated,
        WaitSemaphoreCreated,
        Started
    };
    NFq::NConfig::TRowDispatcherCoordinatorConfig Config;
    const NKikimr::TYdbCredentialsProviderFactory& CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TYdbConnectionPtr YdbConnection;
    TString TablePathPrefix;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;
    TActorId ParentId;
    TActorId CoordinatorId;
    TString LogPrefix;
    const TString Tenant;
    EState State = EState::Init;
    bool CoordinationNodeCreated = false;
    bool SemaphoreCreated = false;
    bool TimeoutScheduled = false;
    bool PendingDescribe = false;
    bool PendingAcquire = false;

    TMaybe<TActorId> LeaderActorId;

    struct NodeInfo {
        bool Connected = false;
    };
    std::map<ui32, NodeInfo> RowDispatchersByNode;
    TLeaderElectionMetrics Metrics;

public:
    TLeaderElection(
        NActors::TActorId parentId,
        NActors::TActorId coordinatorId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();
    void PassAway() override;

    static constexpr char ActorName[] = "YQ_LEADER_EL";

    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvSessionStopped::TPtr& ev);
    void Handle(TEvPrivate::TEvTimeout::TPtr&);
    void Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvOnChangedResult::TPtr& ev);
    void HandleException(const std::exception& e);

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvCreateSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvAcquireSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvOnChangedResult, Handle);
        hFunc(TEvPrivate::TEvSessionStopped, Handle);
        hFunc(TEvPrivate::TEvTimeout, Handle);
        hFunc(TEvPrivate::TEvDescribeSemaphoreResult, Handle);
        cFunc(NActors::TEvents::TSystem::Poison, PassAway);,
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
};

TLeaderElection::TLeaderElection(
    NActors::TActorId parentId,
    NActors::TActorId coordinatorId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , YdbConnection(config.GetLocalMode() ? nullptr : NewYdbConnection(config.GetDatabase(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver))
    , TablePathPrefix(JoinPath(config.GetDatabase().GetDatabase(), config.GetCoordinationNodePath()))
    , CoordinationNodePath(JoinPath(TablePathPrefix, tenant))
    , ParentId(parentId)
    , CoordinatorId(coordinatorId)
    , Tenant(tenant)
    , Metrics(counters) {
}

ERetryErrorClass RetryFunc(const NYdb::TStatus& status) {
    if (status.IsSuccess()) {
        return ERetryErrorClass::NoRetry;
    }

    if (status.IsTransportError()) {
        return ERetryErrorClass::ShortRetry;
    }

    const NYdb::EStatus st = status.GetStatus();
    if (st == NYdb::EStatus::INTERNAL_ERROR || st == NYdb::EStatus::UNAVAILABLE ||
        st == NYdb::EStatus::TIMEOUT || st == NYdb::EStatus::BAD_SESSION ||
        st == NYdb::EStatus::SESSION_EXPIRED ||
        st == NYdb::EStatus::SESSION_BUSY) {
        return ERetryErrorClass::ShortRetry;
    }

    if (st == NYdb::EStatus::OVERLOADED) {
        return ERetryErrorClass::LongRetry;
    }

    return ERetryErrorClass::NoRetry;
}

TYdbSdkRetryPolicy::TPtr MakeSchemaRetryPolicy() {
    static auto policy = TYdbSdkRetryPolicy::GetExponentialBackoffPolicy(RetryFunc, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5));
    return policy;
}

void TLeaderElection::Bootstrap() {
    Become(&TLeaderElection::StateFunc);
    LogPrefix = "TLeaderElection " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped, local coordinator id " << CoordinatorId.ToString());
    if (Config.GetLocalMode()) {
        TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(CoordinatorId, 0));
        return;
    }
    ProcessState();
}

void TLeaderElection::ProcessState() {
    switch (State) {
    case EState::Init:
        if (!CoordinationNodeCreated) {
            Register(MakeCreateCoordinationNodeActor(
                SelfId(),
                NKikimrServices::FQ_ROW_DISPATCHER,
                YdbConnection,
                CoordinationNodePath,
                MakeSchemaRetryPolicy()));
        }
        State = EState::WaitNodeCreated;
        [[fallthrough]];
    case EState::WaitNodeCreated:
        if (!CoordinationNodeCreated) {
            return;
        }
        if (!Session) {
            StartSession();
        }
        State = EState::WaitSessionCreated;
        [[fallthrough]];
    case EState::WaitSessionCreated:
        if (!Session) {
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

void TLeaderElection::ResetState() {
    State = EState::Init;
    SetTimeout();
}

void TLeaderElection::CreateSemaphore() {
    Session->CreateSemaphore(SemaphoreName, 1 /* limit */)
        .Subscribe(
        [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSemaphoreResult(future));
        }); 
}

void TLeaderElection::AcquireSemaphore() {
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
    Session->AcquireSemaphore(
        SemaphoreName,
        NYdb::NCoordination::TAcquireSemaphoreSettings().Count(1).Data(strActorId))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<bool>& future) {
                actorSystem->Send(actorId, new TEvPrivate::TEvAcquireSemaphoreResult(future));
            });
}

void TLeaderElection::StartSession() {
    LOG_ROW_DISPATCHER_DEBUG("Start session");

    YdbConnection->CoordinationClient
        .StartSession(
            CoordinationNodePath, 
            NYdb::NCoordination::TSessionSettings().OnStopped(
                [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()]() {
                actorSystem->Send(actorId, new TEvPrivate::TEvSessionStopped());
            }))
        .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(future));
            });
}

void TLeaderElection::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
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

void TLeaderElection::Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) {
    auto result = ev->Get()->Result.GetValue();
    if (!result.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("CreateSession failed, " << result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    Session =  result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("Session successfully created");
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev) {
    auto result = ev->Get()->Result.GetValue();
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

void TLeaderElection::Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev) {
    auto result = ev->Get()->Result.GetValue();
    PendingAcquire = false;

    if (!result.IsSuccess()) {
        LOG_ROW_DISPATCHER_ERROR("Failed to acquire semaphore, " << result.GetIssues());
        Metrics.Errors->Inc();
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired");
}

void TLeaderElection::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    TActorBootstrapped::PassAway();
}

void TLeaderElection::Handle(TEvPrivate::TEvSessionStopped::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionStopped");
    Session.Clear();
    PendingAcquire = false;
    PendingDescribe = false;
    ResetState();
}

void TLeaderElection::SetTimeout() {
    if (TimeoutScheduled) {
        return;
    }
    TimeoutScheduled = true;
    Schedule(TDuration::Seconds(TimeoutDurationSec), new TEvPrivate::TEvTimeout());
}

void TLeaderElection::Handle(TEvPrivate::TEvTimeout::TPtr&) {
    TimeoutScheduled = false;
    LOG_ROW_DISPATCHER_DEBUG("TEvTimeout");
    ProcessState(); 
}

void TLeaderElection::DescribeSemaphore() {
    if (PendingDescribe) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Describe semaphore");
    PendingDescribe = true;
    Session->DescribeSemaphore(
        SemaphoreName,
        NYdb::NCoordination::TDescribeSemaphoreSettings()
            .WatchData()
            .WatchOwners()
            .IncludeOwners()
            .OnChanged([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](bool isChanged) {
                actorSystem->Send(actorId, new TEvPrivate::TEvOnChangedResult(isChanged));
            }))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncDescribeSemaphoreResult& future) {
                actorSystem->Send(actorId, new TEvPrivate::TEvDescribeSemaphoreResult(future));
            });
}

void TLeaderElection::Handle(TEvPrivate::TEvOnChangedResult::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("Semaphore changed");
    PendingDescribe = false;
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev) {
    PendingDescribe = false;
    auto result = ev->Get()->Result.GetValue();
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
        LOG_ROW_DISPATCHER_INFO("Send TEvCoordinatorChanged to " << ParentId);
        TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(id, generation));
        Metrics.LeaderChanged->Inc();
    }
    LeaderActorId = id;
}

void TLeaderElection::HandleException(const std::exception& e) {
    LOG_ROW_DISPATCHER_ERROR("Internal error: exception:" << e.what());
    Metrics.Errors->Inc();
    ResetState();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderElection(
    NActors::TActorId rowDispatcherId,
    NActors::TActorId coordinatorId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return std::unique_ptr<NActors::IActor>(new TLeaderElection(rowDispatcherId, coordinatorId, config, credentialsProviderFactory, yqSharedResources, tenant, counters));
}

} // namespace NFq
