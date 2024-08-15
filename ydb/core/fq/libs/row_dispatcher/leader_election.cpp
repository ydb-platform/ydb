#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_detector.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSemaphoreResult = EvBegin,
        EvCreateSessionResult,
        EvAcquireSemaphoreResult,
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

    struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, EvAcquireSemaphoreResult> {
        NYdb::NCoordination::TResult<bool> Result;
        explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool> result)
            : Result(std::move(result)) {}
    };
};

////////////////////////////////////////////////////////////////////////////////

class TLeaderElection: public TActorBootstrapped<TLeaderElection> {

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

    static constexpr char ActorName[] = "YQ_LEADER_EL";


    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvCreateSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvAcquireSemaphoreResult, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
    })

private:
    void CreateSemaphore();
    void AcquireSemaphore();
    void DebugPrint();
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
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << SelfId());

    Register(MakeCreateCoordinationNodeActor(
        SelfId(),
        NKikimrServices::YQ_ROW_DISPATCHER,
        YdbConnection,
        CoordinationNodePath,
        MakeSchemaRetryPolicy()));
}

void TLeaderElection::CreateSemaphore() {
    Session->CreateSemaphore("my-semaphore", 1, "my-data")
        .Subscribe(
        [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSemaphoreResult(future.GetValue()));
        }); 
}

void TLeaderElection::AcquireSemaphore() {

    NActorsProto::TActorId protoId;
    ActorIdToProto(CoordinatorId, &protoId);
    TString strActorId;
    if (!protoId.SerializeToString(&strActorId)) {
        Y_ABORT("SerializeToString");
    }
    
    Session->AcquireSemaphore(
        "my-semaphore",
        NYdb::NCoordination::TAcquireSemaphoreSettings().Count(1).Data(strActorId))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<bool>& future) {
                actorSystem->Send(actorId, new TEvPrivate::TEvAcquireSemaphoreResult(future.GetValue()));
            });
}

void TLeaderElection::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_ROW_DISPATCHER_DEBUG("Schema created error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Coordination node successfully created");
    
    YdbConnection->CoordinationClient
        .StartSession(CoordinationNodePath)
        .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(future.GetValue()));
            });
}

void TLeaderElection::Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_DEBUG("StartSession fail, " << ev->Get()->Result.GetIssues());
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("Session successfully created");
    CreateSemaphore();
}

void TLeaderElection::Handle(TEvPrivate::TEvCreateSemaphoreResult::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_ROW_DISPATCHER_DEBUG("Semaphore creating error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully created");
    AcquireSemaphore();
}

void TLeaderElection::Handle(TEvPrivate::TEvAcquireSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_DEBUG("Acquired fail " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired");
}

void TLeaderElection::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChanged ");
    //Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(ev->Get()->CoordinatorActorId));
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
