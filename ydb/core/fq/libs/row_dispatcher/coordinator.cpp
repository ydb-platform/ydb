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

struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, TEvRowDispatcher::EvCreateSemaphoreResult> {
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

    using TPartitionKey = std::tuple<TString, TString, TString, ui64>;     // Endpoint / Database / TopicName / PartitionId 

    NConfig::TRowDispatcherCoordinatorConfig Config;
  //  const NKikimr::TYdbCredentialsProviderFactory& CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
   // TMaybe<TActorId> LeaderActorId;
    TActorId LocalRowDispatcherId;
    const TString LogPrefix;
    const TString Tenant;
    bool IsLeader = false;

    struct RowDispatcherInfo {
        bool Connected = false;
   //     TActorId ActorId;
    };
    TMap<NActors::TActorId, RowDispatcherInfo> RowDispatchers;
    //TVector<NActors::TActorId> RowDispatchers;
    THashMap<TPartitionKey, TActorId> PartitionLocations;

public:
    TActorCoordinator(
        NActors::TActorId localRowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";

    void Handle(NActors::TEvents::TEvPing::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorRequest, Handle);
    })

private:

    void AddRowDispatcher(NActors::TActorId actorId);
    void PrintInternalState();
    NActors::TActorId GetAndUpdateLocation(TPartitionKey key);
};

TActorCoordinator::TActorCoordinator(
    NActors::TActorId localRowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory&, // credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant)
    : Config(config)
   // , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , LocalRowDispatcherId(localRowDispatcherId)
    , LogPrefix("Coordinator: ")
    , Tenant(tenant) {
    AddRowDispatcher(localRowDispatcherId);
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    Send(LocalRowDispatcherId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
}

void TActorCoordinator::AddRowDispatcher(NActors::TActorId actorId) {
    if (RowDispatchers.contains(actorId)) {
        return;
    }
    auto& info = RowDispatchers[actorId];
    info.Connected = true;
}

void TActorCoordinator::Handle(NActors::TEvents::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvPing received, " << ev->Sender);

    AddRowDispatcher(ev->Sender);

    PrintInternalState();
    LOG_ROW_DISPATCHER_TRACE("Send TEvPong to " << ev->Sender);

    Send(ev->Sender, new NActors::TEvents::TEvPong(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TActorCoordinator::PrintInternalState() {
    TStringStream str;
    str << "RowDispatcher:\n";

    for (const auto& [actorId, info] : RowDispatchers) {
        str << "   actorId " << actorId << ", connected " << info.Connected << "\n";
    }

    str << "\nLocations:\n";
    for (auto& [key, actorId] : PartitionLocations) {
        str << "  endpoint: " << std::get<0>(key) << ", db: " << std::get<1>(key) << ", topic " << std::get<2>(key) << ", partId " << std::get<3>(key)  <<  ",  row dispatcher actor id: " << actorId << "\n";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
}

void TActorCoordinator::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected " << ev->Get()->NodeId);

    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Get()->NodeId != actorId.NodeId()) {
            continue;
        }
        info.Connected = true;
    }
}

void TActorCoordinator::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected " << ev->Get()->NodeId);
   
    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Get()->NodeId != actorId.NodeId()) {
            continue;
        }
        info.Connected = false;
    }
}

void TActorCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());

    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Sender != actorId) {
            continue;
        }
        info.Connected = false;
    }
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("new leader " << ev->Get()->CoordinatorActorId);
    LOG_ROW_DISPATCHER_DEBUG("SelfId " << SelfId());

    IsLeader = SelfId() == ev->Get()->CoordinatorActorId;
    LOG_ROW_DISPATCHER_DEBUG("IsLeader " << IsLeader);
}

NActors::TActorId TActorCoordinator::GetAndUpdateLocation(TPartitionKey key) {
    static ui64 counter = 0;

    Y_ENSURE(!PartitionLocations.contains(key));
    auto rand = counter++ % RowDispatchers.size();

    auto it = std::begin(RowDispatchers);
    std::advance(it, rand);

    while(true) {
        auto& info = it->second;
        if (!info.Connected) {
            it++;
            if (it == RowDispatchers.end()) {
                it = RowDispatchers.begin();
            }
            continue;
        }
        PartitionLocations[key] = it->first;
        return it->first;
    }
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorRequest: ");
    const auto source =  ev->Get()->Record.GetSource();
    LOG_ROW_DISPATCHER_DEBUG("  TopicPath " << source.GetTopicPath());
    
    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        LOG_ROW_DISPATCHER_DEBUG("  partitionId " << partitionId);
    }
    Y_ENSURE(!RowDispatchers.empty());

    TMap<NActors::TActorId, TSet<ui64>> tmpResult;

    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        TPartitionKey key{source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath(), partitionId};
        auto locationIt = PartitionLocations.find(key);
        NActors::TActorId rowDispatcherId;
        if (locationIt != PartitionLocations.end()) {
            rowDispatcherId = locationIt->second;
        } else {
            rowDispatcherId = GetAndUpdateLocation(key);
        }
        tmpResult[rowDispatcherId].insert(partitionId);
    }

    auto response = std::make_unique<TEvRowDispatcher::TEvCoordinatorResult>();
    for (auto [actorId, partitions] : tmpResult) {
        auto* partitionsProto = response->Record.AddPartitions();
        ActorIdToProto(actorId, partitionsProto->MutableActorId());
        LOG_ROW_DISPATCHER_DEBUG("  rowDispatcherActorId " << actorId);
        for (auto partitionId : partitions) {
            partitionsProto->AddPartitionId(partitionId);
        }
    }
    
    LOG_ROW_DISPATCHER_DEBUG("Send  TEvCoordinatorResult to " << ev->Sender);
    Send(ev->Sender, response.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    PrintInternalState();
}


} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(rowDispatcherId, config, credentialsProviderFactory, yqSharedResources, tenant));
}

} // namespace NFq
