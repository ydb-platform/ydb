#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
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

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvOnChangedResult = EvBegin,
        EvCreateSessionResult,
        EvDescribeSemaphoreResult,
        EvSessionStopped,
        EvTimeout,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, EvOnChangedResult> {
        bool Result;
        explicit TEvOnChangedResult(bool result)
            : Result(result) {}
    };

    struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
        NYdb::NCoordination::TSessionResult Result;
        explicit TEvCreateSessionResult(NYdb::NCoordination::TSessionResult result)
            : Result(std::move(result)) {}
    };

    struct TEvDescribeSemaphoreResult : NActors::TEventLocal<TEvDescribeSemaphoreResult, EvDescribeSemaphoreResult> {
        NYdb::NCoordination::TDescribeSemaphoreResult Result;
        explicit TEvDescribeSemaphoreResult(NYdb::NCoordination::TDescribeSemaphoreResult result)
            : Result(std::move(result)) {}
    };

    struct TEvTimeout : NActors::TEventLocal<TEvTimeout, EvTimeout> {
    };

    struct TEvSessionStopped : NActors::TEventLocal<TEvSessionStopped, EvSessionStopped> {};
    
};

////////////////////////////////////////////////////////////////////////////////

class TLeaderDetector : public TActorBootstrapped<TLeaderDetector> {

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYdbConnectionPtr YdbConnection;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;
    TActorId ParentId;
    TMaybe<TActorId> LeaderActorId;
    bool HasSubcription = false;
    const TString LogPrefix;
    const TString Tenant;

public:
    TLeaderDetector(
        NActors::TActorId rowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        NYdb::TDriver driver,
        const TString& tenant);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_LEADER_DET";


    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev);
    void Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(TEvPrivate::TEvOnChangedResult::TPtr& ev);
    void Handle(TEvPrivate::TEvSessionStopped::TPtr& ev);
    void Handle(TEvPrivate::TEvTimeout::TPtr&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
        hFunc(TEvPrivate::TEvDescribeSemaphoreResult, Handle);
        hFunc(TEvPrivate::TEvOnChangedResult, Handle);
        hFunc(TEvPrivate::TEvSessionStopped, Handle);
        hFunc(TEvPrivate::TEvTimeout, Handle);

    })

private:
    void ProcessState();
    void StartSession();
    void CreateSemaphore();
    void DescribeSemaphore();
};

TLeaderDetector::TLeaderDetector(
    NActors::TActorId parentId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const TString& tenant)
    : Config(config)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, driver))
    , CoordinationNodePath(JoinPath(YdbConnection->TablePathPrefix, tenant))
    , ParentId(parentId)
    , LogPrefix("LeaderDetector: ")
    , Tenant(tenant) {
}

void TLeaderDetector::Bootstrap() {
    Become(&TLeaderDetector::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped, id " << SelfId());
    ProcessState();
}

void TLeaderDetector::ProcessState() {
    if (!Session) {
        StartSession();
        return;
    }
    if (!HasSubcription) {
        DescribeSemaphore();
    }
}

void TLeaderDetector::StartSession() {
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

void TLeaderDetector::DescribeSemaphore() {
    LOG_ROW_DISPATCHER_DEBUG("Describe semaphore");

    HasSubcription = true;
    Session->DescribeSemaphore(
        "my-semaphore",
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

void TLeaderDetector::Handle(TEvPrivate::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_ROW_DISPATCHER_DEBUG("StartSession fail, " << ev->Get()->Result.GetIssues());
        Schedule(TDuration::Seconds(TimeoutDurationSec), new TEvPrivate::TEvTimeout());
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("Session successfully created");
    ProcessState(); 
}

void TLeaderDetector::Handle(TEvPrivate::TEvDescribeSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        HasSubcription = false;
        LOG_ROW_DISPATCHER_DEBUG("Semaphore describe fail, " <<  ev->Get()->Result.GetIssues());
        Schedule(TDuration::Seconds(TimeoutDurationSec), new TEvPrivate::TEvTimeout());
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Semaphore successfully described:");

    const NYdb::NCoordination::TSemaphoreDescription& description = ev->Get()->Result.GetResult();
    LOG_ROW_DISPATCHER_DEBUG("     name " << description.GetName());
    LOG_ROW_DISPATCHER_DEBUG("     data " << description.GetData());
    LOG_ROW_DISPATCHER_DEBUG("     count " << description.GetCount());
    LOG_ROW_DISPATCHER_DEBUG("     limit " << description.GetLimit());

    for (const auto& owner : description.GetOwners()) {
        LOG_ROW_DISPATCHER_DEBUG("     owner info count " << owner.GetCount());
        LOG_ROW_DISPATCHER_DEBUG("     owner info data " << owner.GetData());
    }
    if (description.GetOwners().empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Empty owners");
        // Wait OnChanged.
        return;
    }
    Y_ABORT_UNLESS(description.GetOwners().size() == 1, "To many owners");
    TString data = description.GetOwners()[0].GetData();

    NActorsProto::TActorId protoId;
    if (!protoId.ParseFromString(data)) {
        Y_ABORT("ParseFromString");
    }

    NActors::TActorId id = ActorIdFromProto(protoId);

    if (!LeaderActorId || (*LeaderActorId != id)) {
        TActivationContext::ActorSystem()->Send(ParentId, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(id));
    }
    LeaderActorId = id;
}

void TLeaderDetector::Handle(TEvPrivate::TEvOnChangedResult::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Semaphore changed,  " << ev->Get()->Result);
    HasSubcription = false;
    ProcessState();
}

void TLeaderDetector::Handle(TEvPrivate::TEvSessionStopped::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionStopped");
    Session.Clear();
    Schedule(TDuration::Seconds(TimeoutDurationSec), new TEvPrivate::TEvTimeout());
}

void TLeaderDetector::Handle(TEvPrivate::TEvTimeout::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvTimeout");
    ProcessState(); 
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderDetector(
    NActors::TActorId parentId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const TString& tenant)
{
    return std::unique_ptr<NActors::IActor>(new TLeaderDetector(parentId, config, credentialsProviderFactory, driver, tenant));
}

} // namespace NFq
