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
    explicit TCoordinatorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        IncomingRequests = Counters->GetCounter("IncomingRequests", true);
        LeaderChanged = Counters->GetCounter("LeaderChanged", true);
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr LeaderChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr IsActive;
};

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    struct TPartitionKey {
        TString Endpoint;
        TString Database;
        TString TopicName;
        ui64 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(Endpoint);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicName));
            hash = CombineHashes<ui64>(hash, std::hash<ui64>()(PartitionId));
            return hash;
        }
        bool operator==(const TPartitionKey& other) const {
            return Endpoint == other.Endpoint && Database == other.Database
                && TopicName == other.TopicName && PartitionId == other.PartitionId;
        }
    };

    struct TPartitionKeyHash {
        int operator()(const TPartitionKey& k) const {
            return k.Hash();
        }
    };

    struct RowDispatcherInfo {
        RowDispatcherInfo(bool connected, bool isLocal) 
            : Connected(connected)
            , IsLocal(isLocal) {}
        bool Connected = false;
        bool IsLocal = false;
        THashSet<TPartitionKey, TPartitionKeyHash> Locations;
    };

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId LocalRowDispatcherId;
    const TString LogPrefix;
    const TString Tenant;
    TMap<NActors::TActorId, RowDispatcherInfo> RowDispatchers;
    THashMap<TPartitionKey, TActorId, TPartitionKeyHash> PartitionLocations;
    TCoordinatorMetrics Metrics;
    ui64 LocationRandomCounter = 0;
    THashSet<TActorId> InterconnectSessions;

public:
    TActorCoordinator(
        NActors::TActorId localRowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();

    static constexpr char ActorName[] = "FQ_RD_COORDINATOR";

    void Handle(NActors::TEvents::TEvPing::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
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

    void AddRowDispatcher(NActors::TActorId actorId, bool isLocal);
    void PrintInternalState();
    NActors::TActorId GetAndUpdateLocation(const TPartitionKey& key);
    void UpdateInterconnectSessions(const NActors::TActorId& interconnectSession);
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
    AddRowDispatcher(localRowDispatcherId, true);
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    Send(LocalRowDispatcherId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
    auto nodeGroup = Metrics.Counters->GetSubgroup("node", ToString(SelfId().NodeId()));
    Metrics.IsActive = nodeGroup->GetCounter("IsActive");
}

void TActorCoordinator::AddRowDispatcher(NActors::TActorId actorId, bool isLocal) {
    auto it = RowDispatchers.find(actorId);
    if (it != RowDispatchers.end()) {
        it->second.Connected = true;
        return;
    }

    for (auto& [oldActorId, info] : RowDispatchers) {
        if (oldActorId.NodeId() != actorId.NodeId()) {
            continue;
        }

        LOG_ROW_DISPATCHER_TRACE("Move all Locations from old actor " << oldActorId.ToString() << " to new " << actorId.ToString());
        for (auto& key : info.Locations) {
            PartitionLocations[key] = actorId;
        }
        info.Connected = true;
        auto node = RowDispatchers.extract(oldActorId);
        node.key() = actorId;
        RowDispatchers.insert(std::move(node));
        return;
    }

    RowDispatchers.emplace(actorId, RowDispatcherInfo{true, isLocal});
}

void TActorCoordinator::UpdateInterconnectSessions(const NActors::TActorId& interconnectSession) {
    if (!interconnectSession) {
        return;
    }
    auto sessionsIt = InterconnectSessions.find(interconnectSession);
    if (sessionsIt != InterconnectSessions.end()) {
        return;
    }
    Send(interconnectSession, new NActors::TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
    InterconnectSessions.insert(interconnectSession);
}

void TActorCoordinator::Handle(NActors::TEvents::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvPing received, " << ev->Sender);
    UpdateInterconnectSessions(ev->InterconnectSession);
    AddRowDispatcher(ev->Sender, false);
    LOG_ROW_DISPATCHER_TRACE("Send TEvPong to " << ev->Sender);
    Send(ev->Sender, new NActors::TEvents::TEvPong(), IEventHandle::FlagTrackDelivery);
}

void TActorCoordinator::PrintInternalState() {
    TStringStream str;
    str << "Known row dispatchers:\n";

    for (const auto& [actorId, info] : RowDispatchers) {
        str << "    " << actorId << ", connected " << info.Connected << "\n";
    }

    str << "\nLocations:\n";
    for (auto& [key, actorId] : PartitionLocations) {
        str << "    " << key.Endpoint << " / " << key.Database << " / " << key.TopicName << ", partId " << key.PartitionId  <<  ",  row dispatcher actor id: " << actorId << "\n";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
}

void TActorCoordinator::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected " << ev->Get()->NodeId);
    // Dont set Connected = true.
    // Wait TEvPing from row dispatchers.
}

void TActorCoordinator::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
   
    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Get()->NodeId != actorId.NodeId()) {
            continue;
        }
        Y_ENSURE(!info.IsLocal, "EvNodeDisconnected from local row dispatcher");
        info.Connected = false;
    }
}

void TActorCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());

    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Sender != actorId) {
            continue;
        }
        info.Connected = false;
        return;
    }
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("New leader " << ev->Get()->CoordinatorActorId << ", SelfId " << SelfId());
    Metrics.LeaderChanged->Inc();

    bool isActive = (ev->Get()->CoordinatorActorId == SelfId());
    Metrics.IsActive->Set(isActive);
}

NActors::TActorId TActorCoordinator::GetAndUpdateLocation(const TPartitionKey& key) {
    Y_ENSURE(!PartitionLocations.contains(key));
    auto rand = LocationRandomCounter++ % RowDispatchers.size();

    auto it = std::begin(RowDispatchers);
    std::advance(it, rand);

    for (size_t i = 0; i < RowDispatchers.size(); ++i) {
        auto& info = it->second;
        if (!info.Connected) {
            it++;
            if (it == std::end(RowDispatchers)) {
                it = std::begin(RowDispatchers);
            }
            continue;
        }
        PartitionLocations[key] = it->first;
        it->second.Locations.insert(key);
        return it->first;
    }
    Y_ENSURE(false, "Local row dispatcher should always be connected");
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev) {
    const auto source =  ev->Get()->Record.GetSource();
    UpdateInterconnectSessions(ev->InterconnectSession);

    TStringStream str;
    str << "TEvCoordinatorRequest from " << ev->Sender.ToString() << ", " << source.GetTopicPath() << ", partIds: ";
    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        str << partitionId << ", ";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
    Metrics.IncomingRequests->Inc();
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
    for (const auto& [actorId, partitions] : tmpResult) {
        auto* partitionsProto = response->Record.AddPartitions();
        ActorIdToProto(actorId, partitionsProto->MutableActorId());
        for (auto partitionId : partitions) {
            partitionsProto->AddPartitionId(partitionId);
        }
    }
    
    LOG_ROW_DISPATCHER_DEBUG("Send TEvCoordinatorResult to " << ev->Sender);
    Send(ev->Sender, response.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
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
