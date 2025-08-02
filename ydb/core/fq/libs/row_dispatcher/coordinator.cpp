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
        PartitionsLimitPerNode = Counters->GetCounter("PartitionsLimitPerNode");
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr LeaderChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr IsActive;
    ::NMonitoring::TDynamicCounters::TCounterPtr PartitionsLimitPerNode;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPrintState = EvBegin,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
};

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    const ui64 PrintStatePeriodSec = 300;
    const ui64 PrintStateToLogSplitSize = 64000;

    struct TTopicKey {
        TString Endpoint;
        TString Database;
        TString TopicName;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(Endpoint);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicName));
            return hash;
        }
        bool operator==(const TTopicKey& other) const {
            return Endpoint == other.Endpoint && Database == other.Database
                && TopicName == other.TopicName;
        }
    };

    struct TTopicKeyHash {
        int operator()(const TTopicKey& k) const {
            return k.Hash();
        }
    };

    struct TPartitionKey {
        TTopicKey Topic;
        ui64 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = Topic.Hash();
            hash = CombineHashes<ui64>(hash, std::hash<ui64>()(PartitionId));
            return hash;
        }
        bool operator==(const TPartitionKey& other) const {
            return Topic == other.Topic && PartitionId == other.PartitionId;
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

    struct TCoordinatorRequest {
        ui64 Cookie;
        NRowDispatcherProto::TEvGetAddressRequest Record;
    };

    struct TTopicInfo {
        struct TTopicMetrics {
            TTopicMetrics(const TCoordinatorMetrics& metrics, const TString& topicNmae)
                : Counters(metrics.Counters->GetSubgroup("topic", topicNmae))
            {
                PendingPartitions = Counters->GetCounter("PendingPartitions");
            }

            ::NMonitoring::TDynamicCounterPtr Counters;
            ::NMonitoring::TDynamicCounters::TCounterPtr PendingPartitions;
        };

        struct TNodeMetrics {
            TNodeMetrics(const TTopicMetrics& metrics, ui32 nodeId)
                : Counters(metrics.Counters->GetSubgroup("node", ToString(nodeId)))
            {
                PartitionsCount = Counters->GetCounter("PartitionsCount");
            }

            ::NMonitoring::TDynamicCounterPtr Counters;
            ::NMonitoring::TDynamicCounters::TCounterPtr PartitionsCount;
        };

        struct TNodeInfo {
            ui64 NumberPartitions = 0;
            TNodeMetrics Metrics;
        };

        TTopicInfo(const TCoordinatorMetrics& metrics, const TString& topicName)
            : Metrics(metrics, topicName)
        {}

        void AddPendingPartition(const TPartitionKey& key) {
            if (PendingPartitions.insert(key).second) {
                Metrics.PendingPartitions->Inc();
            }
        }

        void RemovePendingPartition(const TPartitionKey& key) {
            if (PendingPartitions.erase(key)) {
                Metrics.PendingPartitions->Dec();
            }
        }

        void IncNodeUsage(ui32 nodeId) {
            auto nodeIt = NodesInfo.find(nodeId);
            if (nodeIt == NodesInfo.end()) {
                nodeIt = NodesInfo.insert({nodeId, TNodeInfo{.NumberPartitions = 0, .Metrics = TNodeMetrics(Metrics, nodeId)}}).first;
            }
            nodeIt->second.NumberPartitions++;
            nodeIt->second.Metrics.PartitionsCount->Inc();
        }

        THashSet<TPartitionKey, TPartitionKeyHash> PendingPartitions;
        THashMap<ui32, TNodeInfo> NodesInfo;
        TTopicMetrics Metrics;
    };

    NConfig::TRowDispatcherCoordinatorConfig Config;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId LocalRowDispatcherId;
    const TString LogPrefix;
    const TString Tenant;
    TMap<NActors::TActorId, RowDispatcherInfo> RowDispatchers;
    THashMap<TPartitionKey, TActorId, TPartitionKeyHash> PartitionLocations;
    THashMap<TTopicKey, TTopicInfo, TTopicKeyHash> TopicsInfo;
    std::unordered_map<TActorId, TCoordinatorRequest> PendingReadActors;
    TCoordinatorMetrics Metrics;
    THashSet<TActorId> InterconnectSessions;
    THashMap<TString, ui64> ReadGroupTopicPartitionsLimit;

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
    void Handle(TEvPrivate::TEvPrintState::TPtr&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorRequest, Handle);
        hFunc(TEvPrivate::TEvPrintState, Handle);
    })

private:

    void AddRowDispatcher(NActors::TActorId actorId, bool isLocal);
    void PrintInternalState();
    TTopicInfo& GetOrCreateTopicInfo(const TTopicKey& topic);
    std::optional<TActorId> GetAndUpdateLocation(const TPartitionKey& key, ui64 limitPerNode);  // std::nullopt if TopicPartitionsLimitPerNode reached
    bool ComputeCoordinatorRequest(TActorId readActorId, const TCoordinatorRequest& request);
    void UpdatePendingReadActors();
    void UpdateInterconnectSessions(const NActors::TActorId& interconnectSession);
    TString GetInternalState();
    ui64 GetLimitPerNode(const TString& readGroup);
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
    , Metrics(counters)
{
    Metrics.PartitionsLimitPerNode->Set(Config.GetTopicPartitionsLimitPerNode());
    AddRowDispatcher(localRowDispatcherId, true);
    for (const auto& limitItem : config.GetReadGroupTopicPartitionsLimit()) {
        ReadGroupTopicPartitionsLimit[limitItem.GetReadGroup()] = limitItem.GetLimit();
    }
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    Send(LocalRowDispatcherId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
    auto nodeGroup = Metrics.Counters->GetSubgroup("node", ToString(SelfId().NodeId()));
    Metrics.IsActive = nodeGroup->GetCounter("IsActive");
}

void TActorCoordinator::AddRowDispatcher(NActors::TActorId actorId, bool isLocal) {
    auto it = RowDispatchers.find(actorId);
    if (it != RowDispatchers.end()) {
        it->second.Connected = true;
        UpdatePendingReadActors();
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
        UpdatePendingReadActors();
        return;
    }

    RowDispatchers.emplace(actorId, RowDispatcherInfo{true, isLocal});
    UpdatePendingReadActors();
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

TString TActorCoordinator::GetInternalState() {
    TStringStream str;
    str << "Known row dispatchers:\n";

    for (const auto& [actorId, info] : RowDispatchers) {
        str << "    " << actorId << ", connected " << info.Connected << "\n";
    }

    str << "\nLocations:\n";
    for (auto& [key, actorId] : PartitionLocations) {
        str << "    " << key.Topic.Endpoint << " / " << key.Topic.Database << " / " << key.Topic.TopicName << ", partId " << key.PartitionId  <<  ", row dispatcher actor id: " << actorId << "\n";
    }

    str << "\nPending partitions:\n";
    for (const auto& [topic, topicInfo] : TopicsInfo) {
        str << "    " << topic.TopicName << " (" << topic.Endpoint <<  "), pending partitions: " << topicInfo.PendingPartitions.size() << "\n";
    }
    return str.Str();
}

void TActorCoordinator::PrintInternalState() {
    auto str = GetInternalState();
    auto buf = TStringBuf(str);
    for (ui64 offset = 0; offset < buf.size(); offset += PrintStateToLogSplitSize) {
        LOG_ROW_DISPATCHER_DEBUG(buf.SubString(offset, PrintStateToLogSplitSize));
    }
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

TActorCoordinator::TTopicInfo& TActorCoordinator::GetOrCreateTopicInfo(const TTopicKey& topic) {
    const auto it = TopicsInfo.find(topic);
    if (it != TopicsInfo.end()) {
        return it->second;
    }
    return TopicsInfo.insert({topic, TTopicInfo(Metrics, topic.TopicName)}).first->second;
}

std::optional<TActorId> TActorCoordinator::GetAndUpdateLocation(const TPartitionKey& key, ui64 limitPerNode) {
    Y_ENSURE(!PartitionLocations.contains(key));

    auto& topicInfo = GetOrCreateTopicInfo(key.Topic);

    TActorId bestLocation;
    ui64 bestNumberPartitions = std::numeric_limits<ui64>::max();
    for (auto& [location, info] : RowDispatchers) {
        if (!info.Connected) {
            continue;
        }

        ui64 numberPartitions = 0;
        if (const auto it = topicInfo.NodesInfo.find(location.NodeId()); it != topicInfo.NodesInfo.end()) {
            numberPartitions = it->second.NumberPartitions;
        }

        if (!bestLocation || numberPartitions < bestNumberPartitions) {
            bestLocation = location;
            bestNumberPartitions = numberPartitions;
        }
    }
    Y_ENSURE(bestLocation, "Local row dispatcher should always be connected");

    if (Config.GetTopicPartitionsLimitPerNode() > 0 && bestNumberPartitions >= limitPerNode) {
        topicInfo.AddPendingPartition(key);
        return std::nullopt;
    }

    auto rowDispatcherIt = RowDispatchers.find(bestLocation);
    Y_ENSURE(rowDispatcherIt != RowDispatchers.end(), "Invalid best location");

    PartitionLocations[key] = bestLocation;
    rowDispatcherIt->second.Locations.insert(key);
    topicInfo.IncNodeUsage(bestLocation.NodeId());
    topicInfo.RemovePendingPartition(key);

    return bestLocation;
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev) {
    const auto& source = ev->Get()->Record.GetSource();

    UpdateInterconnectSessions(ev->InterconnectSession);

    TStringStream str;
    LOG_ROW_DISPATCHER_INFO("TEvCoordinatorRequest from " << ev->Sender.ToString() << ", " << source.GetTopicPath() << ", partIds: " << JoinSeq(", ", ev->Get()->Record.GetPartitionIds()));
    Metrics.IncomingRequests->Inc();

    TCoordinatorRequest request = {.Cookie = ev->Cookie, .Record = ev->Get()->Record};
    if (ComputeCoordinatorRequest(ev->Sender, request)) {
        PendingReadActors.erase(ev->Sender);
    } else {
        LOG_ROW_DISPATCHER_INFO("All nodes are overloaded, add request into pending queue");
        // All nodes are overloaded, add request into pending queue
        // We save only last request from each read actor
        PendingReadActors[ev->Sender] = request;
    }
}

ui64 TActorCoordinator::GetLimitPerNode(const TString& readGroup) {
    auto it = ReadGroupTopicPartitionsLimit.find(readGroup);
    if (it != ReadGroupTopicPartitionsLimit.end()) {
        return it->second;
    }
    return Config.GetTopicPartitionsLimitPerNode();
}

bool TActorCoordinator::ComputeCoordinatorRequest(TActorId readActorId, const TCoordinatorRequest& request) {
    const auto& source = request.Record.GetSource();

    Y_ENSURE(!RowDispatchers.empty());

    bool hasPendingPartitions = false;
    ui64 limitPerNode = GetLimitPerNode(source.GetReadGroup());
    
    TMap<NActors::TActorId, TSet<ui64>> tmpResult;
    for (auto& partitionId : request.Record.GetPartitionIds()) {
        TTopicKey topicKey{source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath()};
        TPartitionKey key {topicKey, partitionId};
        auto locationIt = PartitionLocations.find(key);
        NActors::TActorId rowDispatcherId;
        if (locationIt != PartitionLocations.end()) {
            rowDispatcherId = locationIt->second;
        } else {
            if (const auto maybeLocation = GetAndUpdateLocation(key, limitPerNode)) {
                rowDispatcherId = *maybeLocation;
            } else {
                hasPendingPartitions = true;
                continue;
            }
        }
        tmpResult[rowDispatcherId].insert(partitionId);
    }

    if (hasPendingPartitions) {
        return false;
    }

    auto response = std::make_unique<TEvRowDispatcher::TEvCoordinatorResult>();
    for (const auto& [actorId, partitions] : tmpResult) {
        auto* partitionsProto = response->Record.AddPartitions();
        ActorIdToProto(actorId, partitionsProto->MutableActorId());
        for (auto partitionId : partitions) {
            partitionsProto->AddPartitionIds(partitionId);
        }
    }

    LOG_ROW_DISPATCHER_DEBUG("Send TEvCoordinatorResult to " << readActorId);
    Send(readActorId, response.release(), IEventHandle::FlagTrackDelivery, request.Cookie);
    return true;
}

void TActorCoordinator::UpdatePendingReadActors() {
    for (auto readActorIt = PendingReadActors.begin(); readActorIt != PendingReadActors.end();) {
        if (ComputeCoordinatorRequest(readActorIt->first, readActorIt->second)) {
            readActorIt = PendingReadActors.erase(readActorIt);
        } else {
            ++readActorIt;
        }
    }
}

void TActorCoordinator::Handle(TEvPrivate::TEvPrintState::TPtr&) {
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
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
