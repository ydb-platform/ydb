#include "coordinator.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

static const ui64 TimeoutDurationSec = 3;
static const TString SemaphoreName = "RowDispatcher"; 
namespace {

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
        NYdb::NCoordination::TResult<void> Result;
        explicit TEvCreateSemaphoreResult(NYdb::NCoordination::TResult<void> result)
            : Result(std::move(result)) {}
    };
    struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        NYdb::NCoordination::TSessionResult Result;
        explicit TEvCreateSessionResult(NYdb::NCoordination::TSessionResult result)
            : Result(std::move(result)) {}
    };

    struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, EvOnChangedResult> {
        bool Result;
        explicit TEvOnChangedResult(bool result)
            : Result(result) {}
    };

    struct TEvDescribeSemaphoreResult : NActors::TEventLocal<TEvDescribeSemaphoreResult, EvDescribeSemaphoreResult> {
        NYdb::NCoordination::TDescribeSemaphoreResult Result;
        explicit TEvDescribeSemaphoreResult(NYdb::NCoordination::TDescribeSemaphoreResult result)
            : Result(std::move(result)) {}
    };

    struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, EvAcquireSemaphoreResult> {
        NYdb::NCoordination::TResult<bool> Result;
        explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool> result)
            : Result(std::move(result)) {}
    };
    struct TEvSessionStopped : NActors::TEventLocal<TEvSessionStopped, EvSessionStopped> {};
    struct TEvTimeout : NActors::TEventLocal<TEvTimeout, EvTimeout> {};
};

////////////////////////////////////////////////////////////////////////////////

class TLeaderElection: public TActorBootstrapped<TLeaderElection> {

    enum class EState {
        INIT,
        WAIT_NODE_CREATED,
        WAIT_SESSION_CREATED,
        WAIT_SEMAPHORE_CREATED,
        STARTED
    };
    NConfig::TRowDispatcherCoordinatorConfig Config;
    const NKikimr::TYdbCredentialsProviderFactory& CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TYdbConnectionPtr YdbConnection;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;
    TActorId ParentId;
    TActorId CoordinatorId;
    TString LogPrefix;
    const TString Tenant;
    EState State = EState::INIT;
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

public:
    TLeaderElection(
        NActors::TActorId parentId,
        NActors::TActorId coordinatorId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString tenant);

    void Bootstrap();
    void PassAway() override;

    static constexpr char ActorName[] = "YQ_LEADER_EL";

    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev);
   // void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(TEvPrivate::TEvSessionStopped::TPtr& ev);
    void Handle(TEvPrivate::TEvTimeout::TPtr&);
    void Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvOnChangedResult::TPtr& ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvCreateSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvAcquireSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvOnChangedResult, Handle);
        hFunc(TEvPrivate::TEvSessionStopped, Handle);
        hFunc(TEvPrivate::TEvTimeout, Handle);
        hFunc(TEvPrivate::TEvDescribeSemaphoreResult, Handle);
        //hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        cFunc(NActors::TEvents::TSystem::Poison, PassAway);
    })

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
    const TString tenant)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver))
    , CoordinationNodePath(JoinPath(YdbConnection->TablePathPrefix, tenant))
    , ParentId(parentId)
    , CoordinatorId(coordinatorId)
    , LogPrefix("TLeaderElection ")
    , Tenant(tenant) {
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
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped, local coordinator id " << CoordinatorId.ToString());
    ProcessState();
}

void TLeaderElection::ProcessState() {
    switch (State) {
    case EState::INIT:
        if (!CoordinationNodeCreated) {
            Register(MakeCreateCoordinationNodeActor(
                SelfId(),
                NKikimrServices::YQ_ROW_DISPATCHER,
                YdbConnection,
                CoordinationNodePath,
                MakeSchemaRetryPolicy()));
        }
        State = EState::WAIT_NODE_CREATED;
        [[fallthrough]];
    case EState::WAIT_NODE_CREATED:
        if (!CoordinationNodeCreated) {
            return;
        }
        if (!Session) {
            StartSession();
        }
        State = EState::WAIT_SESSION_CREATED;
        [[fallthrough]];
    case EState::WAIT_SESSION_CREATED:
        if (!Session) {
            return;
        }
        if (!SemaphoreCreated) {
            CreateSemaphore();
        }
        State = EState::WAIT_SEMAPHORE_CREATED;
        [[fallthrough]];
    case EState::WAIT_SEMAPHORE_CREATED:
        if (!SemaphoreCreated) {
            return;
        }
        State = EState::STARTED;
        [[fallthrough]];
    case EState::STARTED:
        AcquireSemaphore();
        DescribeSemaphore();
        break;
    }
}

void TLeaderElection::ResetState() {
    State = EState::INIT;
    SetTimeout();
}

void TLeaderElection::CreateSemaphore() {
    Session->CreateSemaphore(SemaphoreName, 1 /* limit */)
        .Subscribe(
        [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSemaphoreResult(future.GetValue()));
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
                actorSystem->Send(actorId, new TEvPrivate::TEvAcquireSemaphoreResult(future.GetValue()));
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
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(future.GetValue()));
            });
}

void TLeaderElection::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_ROW_DISPATCHER_WARN("Schema created error " << ev->Get()->Result.GetIssues());
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Coordination node successfully created");
    CoordinationNodeCreated = true;
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_WARN("StartSession fail, " << ev->Get()->Result.GetIssues());
        ResetState();
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("Session successfully created");
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_ROW_DISPATCHER_WARN("Semaphore creating error " << ev->Get()->Result.GetIssues());
        ResetState();
        return;
    }
    SemaphoreCreated = true;
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully created");
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_WARN("Acquired fail " << ev->Get()->Result.GetIssues());
        PendingAcquire = false;
        ResetState();
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired");
}

// void TLeaderElection::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& /*ev*/) {
//     LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChanged ");
//     //Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(ev->Get()->CoordinatorActorId));
// }

void TLeaderElection::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    TActorBootstrapped::PassAway();
}

void TLeaderElection::Handle(TEvPrivate::TEvSessionStopped::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionStopped");
    Session.Clear();
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
                actorSystem->Send(actorId, new TEvPrivate::TEvDescribeSemaphoreResult(future.GetValue()));
            });
}

void TLeaderElection::Handle(TEvPrivate::TEvOnChangedResult::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("Semaphore changed");
    PendingDescribe = false;
    ProcessState();
}

void TLeaderElection::Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_WARN("Semaphore describe fail, " <<  ev->Get()->Result.GetIssues());
        PendingDescribe = false;
        ResetState();
        return;
    }

    const NYdb::NCoordination::TSemaphoreDescription& description = ev->Get()->Result.GetResult();
    Y_ABORT_UNLESS(description.GetOwners().size() == 1, "To many owners");
    if (description.GetOwners().empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Empty owners");
        // Wait OnChanged.
        return;
    }
    TString data = description.GetOwners()[0].GetData();
    NActorsProto::TActorId protoId;
    if (!protoId.ParseFromString(data)) {
        Y_ABORT("ParseFromString");
    }

    NActors::TActorId id = ActorIdFromProto(protoId);
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully described: coordinator id " << id);
    if (!LeaderActorId || (*LeaderActorId != id)) {
        LOG_ROW_DISPATCHER_INFO("Send TEvCoordinatorChanged to " << ParentId);
        TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(id));
    }
    LeaderActorId = id;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderElection(
    NActors::TActorId rowDispatcherId,
    NActors::TActorId coordinatorId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant)
{
    return std::unique_ptr<NActors::IActor>(new TLeaderElection(rowDispatcherId, coordinatorId, config, credentialsProviderFactory, yqSharedResources, tenant));
}

} // namespace NFq
