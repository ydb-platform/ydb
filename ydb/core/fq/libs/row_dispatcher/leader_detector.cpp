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

struct TEvOnChangedResult : NActors::TEventLocal<TEvOnChangedResult, TEventIds::EvEndpointResponse> {
    bool Result;

    explicit TEvOnChangedResult(bool result)
        : Result(result)
    {}
};


struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, TEventIds::EvCreateSemaphoreResult> {
    NYdb::NCoordination::TSessionResult Result;

    explicit TEvCreateSessionResult(NYdb::NCoordination::TSessionResult result)
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

class TLeaderDetector : public TActorBootstrapped<TLeaderDetector> {

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYdbConnectionPtr YdbConnection;
    TString CoordinationNodePath;
    TMaybe<NYdb::NCoordination::TSession> Session;
    TActorId ParentId;
    TMaybe<TActorId> LeaderActorId;
    bool HasSubcription = false; 

public:
    TLeaderDetector(
        NActors::TActorId rowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";


    void Handle(NFq::TEvCreateSessionResult::TPtr& ev);
    void Handle(NFq::TEvDescribeSemaphoreResult::TPtr& ev);
    void Handle(NFq::TEvOnChangedResult::TPtr& ev);
    void Handle(NActors::TEvents::TEvWakeup::TPtr&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvCreateSessionResult, Handle);
        hFunc(NFq::TEvDescribeSemaphoreResult, Handle);
        hFunc(NFq::TEvOnChangedResult, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);

    })

private:
    void StartSession();
    void CreateSemaphore();
    void DescribeSemaphore();
};

TLeaderDetector::TLeaderDetector(
    NActors::TActorId parentId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, yqSharedResources->UserSpaceYdbDriver))
    , CoordinationNodePath(JoinPath(YdbConnection->TablePathPrefix, Config.GetNodePath()))
    , ParentId(parentId) {
}

void TLeaderDetector::Bootstrap() {
    Become(&TLeaderDetector::StateFunc);
    LOG_YQ_ROW_DISPATCHER_DEBUG("9 Successfully bootstrapped coordinator, id " << SelfId());

    StartSession();
}

void TLeaderDetector::StartSession() {
    YdbConnection->CoordinationClient
        .StartSession(CoordinationNodePath)
        .Subscribe([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncSessionResult& future) {
                actorSystem->Send(actorId, new TEvCreateSessionResult(future.GetValue()));
            });
}

void TLeaderDetector::DescribeSemaphore() {

    LOG_YQ_ROW_DISPATCHER_DEBUG("9 DescribeSemaphore");

    HasSubcription = true;
    Session->DescribeSemaphore(
        "my-semaphore",
        NYdb::NCoordination::TDescribeSemaphoreSettings()
            .WatchData()
            .WatchOwners()
            .IncludeOwners()
            .OnChanged([actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](bool isChanged) {
                actorSystem->Send(actorId, new TEvOnChangedResult(isChanged));
            }))
        .Subscribe(
            [actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const NYdb::NCoordination::TAsyncDescribeSemaphoreResult& future) {
                actorSystem->Send(actorId, new TEvDescribeSemaphoreResult(future.GetValue()));
            });
}

void TLeaderDetector::Handle(NFq::TEvCreateSessionResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("9 StartSession fail, " << ev->Get()->Result.GetIssues());
        Schedule(TDuration::Seconds(3), new NActors::TEvents::TEvWakeup());
        return;
    }
    Session =  ev->Get()->Result.GetResult();
    LOG_YQ_ROW_DISPATCHER_DEBUG("9 Session successfully created");
    DescribeSemaphore(); 
}

void TLeaderDetector::Handle(NFq::TEvDescribeSemaphoreResult::TPtr& ev) {
    if (!ev->Get()->Result.IsSuccess()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("9 Semaphore describe fail, " <<  ev->Get()->Result.GetIssues());
        HasSubcription = false;
        Schedule(TDuration::Seconds(3), new NActors::TEvents::TEvWakeup());
        return;
    }
    LOG_YQ_ROW_DISPATCHER_DEBUG("9 Semaphore successfully described:");

    const NYdb::NCoordination::TSemaphoreDescription& description = ev->Get()->Result.GetResult();
    LOG_YQ_ROW_DISPATCHER_DEBUG("9     name " << description.GetName());
    LOG_YQ_ROW_DISPATCHER_DEBUG("9     data " << description.GetData());
    LOG_YQ_ROW_DISPATCHER_DEBUG("9     count " << description.GetCount());
    LOG_YQ_ROW_DISPATCHER_DEBUG("9     limit " << description.GetLimit());

    for (const auto& owner : description.GetOwners()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("9     owner info count " << owner.GetCount());
        LOG_YQ_ROW_DISPATCHER_DEBUG("9     owner info data " << owner.GetData());
    }
    if (description.GetOwners().empty()) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("9 Empty owners !!! ");
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

void TLeaderDetector::Handle(NFq::TEvOnChangedResult::TPtr& ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("9 Semaphore changed,  " << ev->Get()->Result);

    // TODO: false?
    
    if (ev->Get()->Result) {
        DescribeSemaphore();
    }
}

void TLeaderDetector::Handle(NActors::TEvents::TEvWakeup::TPtr&) {
    if (!Session) {
        StartSession();
        return;
    }
    if (!HasSubcription) {
        DescribeSemaphore();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewLeaderDetector(
    NActors::TActorId parentId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TLeaderDetector(parentId, config, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
