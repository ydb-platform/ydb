#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
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

namespace {


struct TEvCreateSemaphoreResult : NActors::TEventLocal<TEvCreateSemaphoreResult, TEventIds::EvDeleteRateLimiterResourceResponse> {
    NYdb::NCoordination::TResult<void> Result;

    explicit TEvCreateSemaphoreResult(NYdb::NCoordination::TResult<void> result)
        : Result(std::move(result))
    {}
};

struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, TEventIds::EvCreateSemaphoreResult> {
    NYdb::NCoordination::TSessionResult Result;

    explicit TEvCreateSessionResult(NYdb::NCoordination::TSessionResult result)
        : Result(std::move(result))
    {}
};

struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, TEventIds::EvSchemaUpdated> { // TODO
    NYdb::NCoordination::TResult<bool> Result;

    explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool> result)
        : Result(std::move(result))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYdbConnectionPtr YdbConnection;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;
    TActorId RowDispatcherActorId;
    TMaybe<TActorId> LeaderActorId;

public:
    TActorCoordinator(
        NActors::TActorId rowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";


    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(NFq::TEvCreateSessionResult::TPtr& ev);
    void Handle(NFq::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvAcquireSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) ;

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(NFq::TEvCreateSessionResult, Handle);
        hFunc(NFq::TEvCreateSemaphoreResult, Handle);
        hFunc(NFq::TEvAcquireSemaphoreResult, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    })

private:
    void CreateSemaphore();
    void AcquireSemaphore();
};

TActorCoordinator::TActorCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver))
    , CoordinationNodePath(JoinPath(YdbConnection->TablePathPrefix, Config.GetNodePath()))
    , RowDispatcherActorId(rowDispatcherId) {
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

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Successfully bootstrapped coordinator, id " << SelfId());

    Register(MakeCreateCoordinationNodeActor(
        SelfId(),
        NKikimrServices::YQ_ROW_DISPATCHER,
        YdbConnection,
        CoordinationNodePath,
        MakeSchemaRetryPolicy()));
}

void TActorCoordinator::CreateSemaphore() {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: CreateSemaphore");

    Session->CreateSemaphore("my-semaphore", 1, "my-data")
        .Subscribe(
        [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
            actorSystem->Send(actorId, new TEvCreateSemaphoreResult(future.GetValue()));
        }); 
}

void TActorCoordinator::AcquireSemaphore() {
    LOG_YQ_ROW_DISPATCHER_DEBUG("AcquireSemaphore");

    NActorsProto::TActorId protoId;
    ActorIdToProto(SelfId(), &protoId);
    TString strActorId;
    if (!protoId.SerializeToString(&strActorId)) {
        Y_ABORT("SerializeToString");
    }
    
    Session->AcquireSemaphore(
        "my-semaphore",
        NYdb::NCoordination::TAcquireSemaphoreSettings().Count(1).Data(strActorId))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<bool>& future) {
                actorSystem->Send(actorId, new TEvAcquireSemaphoreResult(future.GetValue()));
            });
}

void TActorCoordinator::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Schema created error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Coordination node successfully created");
    
    YdbConnection->CoordinationClient
        .StartSession(CoordinationNodePath)
        .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
                actorSystem->Send(actorId, new TEvCreateSessionResult(future.GetValue()));
            });
}

void TActorCoordinator::Handle(NFq::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {

        LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: StartSession fail, " << ev->Get()->Result.GetIssues());
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Session successfully created");
    CreateSemaphore();
}

void TActorCoordinator::Handle(NFq::TEvCreateSemaphoreResult::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Semaphore creating error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Semaphore successfully created");
    AcquireSemaphore();
}

void TActorCoordinator::Handle(NFq::TEvAcquireSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Acquired fail " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: Semaphore successfully acquired");
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: StartSession received, " << ev->Sender);

    Send(ev->Sender, new TEvRowDispatcher::TEvCoordinatorInfo(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TActorCoordinator::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: EvNodeConnected " << ev->Get()->NodeId);
}


void TActorCoordinator::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: TEvNodeDisconnected " << ev->Get()->NodeId);
}

void TActorCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {

    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordinator: TEvUndelivered, Reason: " << ev->Get()->Reason);
    //Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
}
} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRDCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(rowDispatcherId, config, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
