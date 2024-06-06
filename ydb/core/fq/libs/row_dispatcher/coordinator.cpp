#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {

struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, TEventIds::EvEndpointResponse> {
    bool Result;

    explicit TEvOnChangedResult(bool result)
        : Result(result)
    {}
};


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


struct TEvDescribeSemaphoreResult : NActors::TEventLocal<TEvDescribeSemaphoreResult, TEventIds::EvSchemaDeleted> {  // TODO
    NYdb::NCoordination::TDescribeSemaphoreResult Result;

    explicit TEvDescribeSemaphoreResult(NYdb::NCoordination::TDescribeSemaphoreResult result)
        : Result(std::move(result))
    {}
};
////////////////////////////////////////////////////////////////////////////////

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {


    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYdbConnectionPtr YdbConnection;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;

public:
    TActorCoordinator(
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";


    void Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev);
    void Handle(NFq::TEvCreateSessionResult::TPtr& ev);
    void Handle(NFq::TEvCreateSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvAcquireSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvOnChangedResult::TPtr& ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvents::TEvSchemaCreated, Handle);
        hFunc(NFq::TEvCreateSessionResult, Handle);
        hFunc(NFq::TEvCreateSemaphoreResult, Handle);
        hFunc(NFq::TEvAcquireSemaphoreResult, Handle);
        hFunc(NFq::TEvDescribeSemaphoreResult, Handle);
        hFunc(NFq::TEvOnChangedResult, Handle);
    })

private:
    void CreateSemaphore();
    void AcquireSemaphore();
    void DescribeSemaphore();
};

TActorCoordinator::TActorCoordinator(
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver))
    , CoordinationNodePath(JoinPath(YdbConnection->TablePathPrefix, Config.GetNodePath())) {
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
    LOG_YQ_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());

    Register(MakeCreateCoordinationNodeActor(
        SelfId(),
        NKikimrServices::YQ_ROW_DISPATCHER,
        YdbConnection,
        CoordinationNodePath,
        MakeSchemaRetryPolicy()));
}

void TActorCoordinator::CreateSemaphore() {
    LOG_YQ_ROW_DISPATCHER_DEBUG("CreateSemaphore");

    Session->CreateSemaphore("my-semaphore", 2, "my-data")
        .Subscribe(
        [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<void>& future) {
            actorSystem->Send(actorId, new TEvCreateSemaphoreResult(future.GetValue()));
        }); 
}

void TActorCoordinator::DescribeSemaphore() {

    LOG_YQ_ROW_DISPATCHER_DEBUG("DescribeSemaphore");

    Session->DescribeSemaphore(
        "my-semaphore",
        NYdb::NCoordination::TDescribeSemaphoreSettings()
            .WatchData()
            .WatchOwners()
            .OnChanged([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](bool isChanged) {
                actorSystem->Send(actorId, new TEvOnChangedResult(isChanged));
            }))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncDescribeSemaphoreResult& future) {
                actorSystem->Send(actorId, new TEvDescribeSemaphoreResult(future.GetValue()));
            });
}

void TActorCoordinator::AcquireSemaphore() {
    LOG_YQ_ROW_DISPATCHER_DEBUG("AcquireSemaphore");

    TString leaderActorId = SelfId().ToString();

    LOG_YQ_ROW_DISPATCHER_DEBUG("  leaderActorId " << leaderActorId);
    
    Session->AcquireSemaphore(
        "my-semaphore",
        NYdb::NCoordination::TAcquireSemaphoreSettings().Count(1).Data(leaderActorId))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncResult<bool>& future) {
                actorSystem->Send(actorId, new TEvAcquireSemaphoreResult(future.GetValue()));
            });
}

void TActorCoordinator::Handle(NFq::TEvents::TEvSchemaCreated::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Schema created error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Coordination node successfully created");
    
    YdbConnection->CoordinationClient
        .StartSession(CoordinationNodePath)
        .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
                actorSystem->Send(actorId, new TEvCreateSessionResult(future.GetValue()));
            });
}

void TActorCoordinator::Handle(NFq::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("StartSession fail, " << ev->Get()->Result.GetIssues());
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_YQ_ROW_DISPATCHER_DEBUG("Session successfully created");
    CreateSemaphore();
}

void TActorCoordinator::Handle(NFq::TEvCreateSemaphoreResult::TPtr& ev) {
    if (!IsTableCreated(ev->Get()->Result)) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore creating error " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore successfully created");
    AcquireSemaphore();
    DescribeSemaphore(); 
}

void TActorCoordinator::Handle(NFq::TEvAcquireSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Acquired fail " << ev->Get()->Result.GetIssues());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore successfully acquired");
}

void TActorCoordinator::Handle(NFq::TEvDescribeSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore describe fail");
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore successfully described:");

    const NYdb::NCoordination::TSemaphoreDescription& description = ev->Get()->Result.GetResult();
    LOG_YQ_ROW_DISPATCHER_DEBUG("    name " << description.GetName());
    LOG_YQ_ROW_DISPATCHER_DEBUG("    data " << description.GetData());
    LOG_YQ_ROW_DISPATCHER_DEBUG("    count " << description.GetCount());
    LOG_YQ_ROW_DISPATCHER_DEBUG("    limit " << description.GetLimit());
    for (const auto& owner : description.GetOwners()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("    owner info count " << owner.GetCount());
        LOG_YQ_ROW_DISPATCHER_DEBUG("    owner info data " << owner.GetData());
    }
}

void TActorCoordinator::Handle(NFq::TEvOnChangedResult::TPtr& ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("Semaphore changed,  " << ev->Get()->Result);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRDCoordinator(
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(config, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
