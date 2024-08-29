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

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorMetrics {
    TCoordinatorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        IncomingRequests = Counters->GetCounter("IncomingRequests");
        LeaderChangedCount = Counters->GetCounter("LeaderChangedCount");
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr LeaderChangedCount;

};

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    using TPartitionKey = std::tuple<TString, TString, TString, ui64>;     // Endpoint / Database / TopicName / PartitionId 

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId LocalRowDispatcherId;
    const TString LogPrefix;
    const TString Tenant;

    struct RowDispatcherInfo {
        bool Connected = false;
        TSet<TPartitionKey> Locations;
    };
    TMap<NActors::TActorId, RowDispatcherInfo> RowDispatchers;
    THashMap<TPartitionKey, TActorId> PartitionLocations;
    TCoordinatorMetrics Metrics;

public:
    TActorCoordinator(
        NActors::TActorId localRowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant,
        const ::NMonitoring::TDynamicCounterPtr& counters);

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
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : Config(config)
    , YqSharedResources(yqSharedResources)
    , LocalRowDispatcherId(localRowDispatcherId)
    , LogPrefix("Coordinator: ")
    , Tenant(tenant)
    , Metrics(counters) {
    AddRowDispatcher(localRowDispatcherId);
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    Send(LocalRowDispatcherId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
}

void TActorCoordinator::AddRowDispatcher(NActors::TActorId actorId) {
    auto it = RowDispatchers.find(actorId);
    if (it != RowDispatchers.end()) {
        it->second.Connected = true;
        return;
    }

    for (auto& [oldActorId, info] : RowDispatchers) {
        if (oldActorId.NodeId() != actorId.NodeId()) {
            continue;
        }

        LOG_ROW_DISPATCHER_TRACE(" Move all Locations from old actor " << oldActorId.ToString() << " to new " << actorId.ToString());
        for (auto& key : info.Locations) {
            PartitionLocations[key] = actorId;
        }
        info.Connected = true;
        auto node = RowDispatchers.extract(oldActorId);
        node.key() = actorId;
        RowDispatchers.insert(std::move(node));
        return;
    }
    RowDispatchers[actorId].Connected = true;
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
    str << "Known row dispatchers:\n";

    for (const auto& [actorId, info] : RowDispatchers) {
        str << "    " << actorId << ", connected " << info.Connected << "\n";
    }

    str << "\nLocations:\n";
    for (auto& [key, actorId] : PartitionLocations) {
        str << "    " << std::get<0>(key) << " / " << std::get<1>(key) << " / " << std::get<2>(key) << ", partId " << std::get<3>(key)  <<  ",  row dispatcher actor id: " << actorId << "\n";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
}

void TActorCoordinator::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected " << ev->Get()->NodeId);
    // Dont set Connected = false.
    // Wait TEvPing from row dispatchers.
}

void TActorCoordinator::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
   
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
    LOG_ROW_DISPATCHER_DEBUG("New leader " << ev->Get()->CoordinatorActorId << ", SelfId " << SelfId());
    Metrics.LeaderChangedCount->Inc();
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
        it->second.Locations.insert(key);
        return it->first;
    }
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev) {
    const auto source =  ev->Get()->Record.GetSource();
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorRequest from " << ev->Sender.ToString() << ", " << source.GetTopicPath());
    Metrics.IncomingRequests->Inc();
    
    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        LOG_ROW_DISPATCHER_TRACE("  partitionId " << partitionId);
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
    Send(ev->Sender, response.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ev->Cookie);
    PrintInternalState();
}


} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(rowDispatcherId, config, yqSharedResources, tenant, counters));
}

} // namespace NFq
