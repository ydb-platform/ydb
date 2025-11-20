#include "coordinator.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/actors/nodes_manager_events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ydb/core/mind/tenant_node_enumeration.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {

const ui64 DefaultRebalancingTimeoutSec = 120;
    
////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorMetrics {
    explicit TCoordinatorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        IncomingRequests = Counters->GetCounter("IncomingRequests", true);
        LeaderChanged = Counters->GetCounter("LeaderChanged", true);
        KnownRowDispatchers = Counters->GetCounter("KnownRowDispatchers");
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr LeaderChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr IsActive;
    ::NMonitoring::TDynamicCounters::TCounterPtr KnownRowDispatchers;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPrintState = EvBegin,
        EvListNodes,
        EvRebalancing,
        EvStartingTimeout,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
    struct TEvListNodes : public NActors::TEventLocal<TEvListNodes, EvListNodes> {};
    struct TEvRebalancing : public NActors::TEventLocal<TEvRebalancing, EvRebalancing> {};
    struct TEvStartingTimeout : public NActors::TEventLocal<TEvStartingTimeout, EvStartingTimeout> {};
};

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    static constexpr ui64 PrintStatePeriodSec = 300;
    static constexpr ui64 PrintStateToLogSplitSize = 64000;
    static constexpr TDuration NodesManagerRetryPeriod = TDuration::Seconds(10);

    struct TTopicKey {
        TString Endpoint;
        TString Database;
        TString ReadGroup;
        TString TopicName;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(Endpoint);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(ReadGroup));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicName));
            return hash;
        }
        bool operator==(const TTopicKey& other) const {
            return Endpoint == other.Endpoint && Database == other.Database
                && ReadGroup == other.ReadGroup && TopicName == other.TopicName;
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

    enum class ENodeState {
        Initializing,   // wait timeout after connected
        Started
    };

    struct TRowDispatcherInfo {
        TRowDispatcherInfo(bool connected, ENodeState state, bool isLocal) 
            : Connected(connected)
            , State(state)
            , IsLocal(isLocal) {}
        bool Connected = false;
        ENodeState State; 
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
    TMap<NActors::TActorId, TRowDispatcherInfo> RowDispatchers;
    THashMap<TPartitionKey, TActorId, TPartitionKeyHash> PartitionLocations;
    THashMap<TTopicKey, TTopicInfo, TTopicKeyHash> TopicsInfo;
    std::unordered_map<TActorId, TCoordinatorRequest> PendingReadActors;
    std::unordered_set<TActorId> KnownReadActors;
    TCoordinatorMetrics Metrics;
    THashSet<TActorId> InterconnectSessions;
    ui64 NodesCount = 0;
    NActors::TActorId NodesManagerId;
    bool RebalancingScheduled = false;
    ENodeState State = ENodeState::Initializing;
    TDuration RebalancingTimeout;

public:
    TActorCoordinator(
        NActors::TActorId localRowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        NActors::TActorId nodesManagerId);

    void Bootstrap();

    static constexpr char ActorName[] = "FQ_RD_COORDINATOR";

    void Handle(NActors::TEvents::TEvPing::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev);
    void Handle(TEvPrivate::TEvPrintState::TPtr&);
    void Handle(TEvPrivate::TEvListNodes::TPtr&);
    void Handle(TEvPrivate::TEvRebalancing::TPtr&);
    void Handle(TEvPrivate::TEvStartingTimeout::TPtr&);
    void Handle(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult::TPtr&);
    void Handle(NFq::TEvNodesManager::TEvGetNodesResponse::TPtr&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorRequest, Handle);
        hFunc(TEvPrivate::TEvPrintState, Handle);
        hFunc(TEvPrivate::TEvListNodes, Handle);
        hFunc(TEvPrivate::TEvRebalancing, Handle);
        hFunc(TEvPrivate::TEvStartingTimeout, Handle);
        hFunc(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        hFunc(NFq::TEvNodesManager::TEvGetNodesResponse, Handle);
    })

private:

    void UpdateKnownRowDispatchers(NActors::TActorId actorId, bool isLocal);
    void PrintInternalState();
    TTopicInfo& GetOrCreateTopicInfo(const TTopicKey& topic);
    std::optional<TActorId> GetAndUpdateLocation(const TPartitionKey& key, const TSet<ui32>& filteredNodeIds);  // std::nullopt if TopicPartitionsLimitPerNode reached
    bool ComputeCoordinatorRequest(TActorId readActorId, const TCoordinatorRequest& request);
    void UpdatePendingReadActors();
    void UpdateInterconnectSessions(const NActors::TActorId& interconnectSession);
    TString GetInternalState();
    bool IsReadyPartitionDistribution() const;
    void SendError(TActorId readActorId, const TCoordinatorRequest& request, const TString& message);
    void ScheduleNodeInfoRequest() const;
    void UpdateGlobalState();
};

TActorCoordinator::TActorCoordinator(
    NActors::TActorId localRowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    NActors::TActorId nodesManagerId)
    : Config(config)
    , YqSharedResources(yqSharedResources)
    , LocalRowDispatcherId(localRowDispatcherId)
    , LogPrefix("Coordinator: ")
    , Tenant(tenant)
    , Metrics(counters)
    , NodesManagerId(nodesManagerId)
    , RebalancingTimeout(TDuration::Seconds(Config.GetRebalancingTimeoutSec() ? Config.GetRebalancingTimeoutSec() : DefaultRebalancingTimeoutSec))
{
    UpdateKnownRowDispatchers(localRowDispatcherId, true);
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    Send(LocalRowDispatcherId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    ScheduleNodeInfoRequest();
    Schedule(RebalancingTimeout, new TEvPrivate::TEvStartingTimeout());
    // Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());  // Logs (InternalState) is too big
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId() 
        << ", NodesManagerId " << NodesManagerId
        << ", rebalancing timeout " << RebalancingTimeout);
    auto nodeGroup = Metrics.Counters->GetSubgroup("node", ToString(SelfId().NodeId()));
    Metrics.IsActive = nodeGroup->GetCounter("IsActive");
}

void TActorCoordinator::UpdateKnownRowDispatchers(NActors::TActorId actorId, bool isLocal) {
    LOG_ROW_DISPATCHER_TRACE("UpdateKnownRowDispatchers " << actorId.ToString());

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
    auto nodeState = State == ENodeState::Initializing ? ENodeState::Started : ENodeState::Initializing;
    if (PartitionLocations.empty()) {
        nodeState = ENodeState::Started;
    }

    LOG_ROW_DISPATCHER_TRACE("Add new row dispatcher to map (state " << static_cast<int>(nodeState) << ")");
    RowDispatchers.emplace(actorId, TRowDispatcherInfo{true, nodeState, isLocal});
    UpdateGlobalState();

    if (nodeState == ENodeState::Initializing && !RebalancingScheduled) {
        LOG_ROW_DISPATCHER_TRACE("Schedule TEvRebalancing");
        Schedule(RebalancingTimeout, new TEvPrivate::TEvRebalancing());
        RebalancingScheduled = true;
    }

    UpdatePendingReadActors();
    Metrics.KnownRowDispatchers->Set(RowDispatchers.size());
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
    UpdateKnownRowDispatchers(ev->Sender, false);
    LOG_ROW_DISPATCHER_TRACE("Send TEvPong to " << ev->Sender);
    Send(ev->Sender, new NActors::TEvents::TEvPong(), IEventHandle::FlagTrackDelivery);
}

TString TActorCoordinator::GetInternalState() {
    TStringStream str;
    str << "Known row dispatchers:\n";

    for (const auto& [actorId, info] : RowDispatchers) {
        str << "    " << actorId << ", state " << static_cast<int>(info.State) << "\n";
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

        if (!RebalancingScheduled) {
            LOG_ROW_DISPATCHER_TRACE("Schedule TEvRebalancing");
            Schedule(RebalancingTimeout, new TEvPrivate::TEvRebalancing());
            RebalancingScheduled = true;
        }
    }
}

void TActorCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());

    if (ev->Sender == NodesManagerId) {
        LOG_ROW_DISPATCHER_INFO("TEvUndelivered, from nodes manager, reason: " << ev->Get()->Reason);
        NActors::TActivationContext::Schedule(NodesManagerRetryPeriod, new IEventHandle(NodesManagerId, SelfId(), new NFq::TEvNodesManager::TEvGetNodesRequest(), IEventHandle::FlagTrackDelivery));
        return;
    }

    for (auto& [actorId, info] : RowDispatchers) {
        if (ev->Sender != actorId) {
            continue;
        }
        info.Connected = false;

        if (!RebalancingScheduled) {
            LOG_ROW_DISPATCHER_TRACE("Schedule TEvRebalancing");
            Schedule(RebalancingTimeout, new TEvPrivate::TEvRebalancing());
            RebalancingScheduled = true;
        }
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

std::optional<TActorId> TActorCoordinator::GetAndUpdateLocation(const TPartitionKey& key, const TSet<ui32>& filteredNodeIds) {
    Y_ENSURE(!PartitionLocations.contains(key));

    auto& topicInfo = GetOrCreateTopicInfo(key.Topic);

    if (!IsReadyPartitionDistribution()) {
        topicInfo.AddPendingPartition(key);
        return std::nullopt;
    }

    TActorId bestLocation;
    ui64 bestNumberPartitions = std::numeric_limits<ui64>::max();
    for (auto& [location, info] : RowDispatchers) {
        if (info.State != ENodeState::Started) {
            continue;
        }
        if (!filteredNodeIds.empty() && !filteredNodeIds.contains(location.NodeId())) {
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
    if (!bestLocation) {
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

    KnownReadActors.insert(ev->Sender);

    UpdateInterconnectSessions(ev->InterconnectSession);

    TStringStream str;
    LOG_ROW_DISPATCHER_INFO("TEvCoordinatorRequest from " << ev->Sender.ToString() << ", " << source.GetTopicPath() << ", partIds: " << JoinSeq(", ", ev->Get()->Record.GetPartitionIds()));
    Metrics.IncomingRequests->Inc();

    TCoordinatorRequest request = {.Cookie = ev->Cookie, .Record = ev->Get()->Record};
    if (ComputeCoordinatorRequest(ev->Sender, request)) {
        PendingReadActors.erase(ev->Sender);
    } else {
        LOG_ROW_DISPATCHER_INFO("Not all nodes connected, nodes count: " << NodesCount << ", known rd count: " << RowDispatchers.size() << ", add request into pending queue");
        // We save only last request from each read actor
        PendingReadActors[ev->Sender] = request;
    }
}

bool TActorCoordinator::ComputeCoordinatorRequest(TActorId readActorId, const TCoordinatorRequest& request) {
    const auto& source = request.Record.GetSource();
    TSet<ui32> filteredNodeIds{source.GetNodeIds().begin(), source.GetNodeIds().end()};
    Y_ENSURE(!RowDispatchers.empty());

    bool hasPendingPartitions = false;
    TMap<NActors::TActorId, TSet<ui64>> tmpResult;
    for (auto& partitionId : request.Record.GetPartitionIds()) {
        TTopicKey topicKey{source.GetEndpoint(), source.GetDatabase(), source.GetReadGroup(), source.GetTopicPath()};
        TPartitionKey key {topicKey, partitionId};
        auto locationIt = PartitionLocations.find(key);
        NActors::TActorId rowDispatcherId;
        if (locationIt != PartitionLocations.end()) {
            rowDispatcherId = locationIt->second;
            if (!filteredNodeIds.empty() && !filteredNodeIds.contains(rowDispatcherId.NodeId())) {
                SendError(readActorId, request, TStringBuilder() << "Can't read the same topic with different mappings");
                return true;
            }
        } else {
            if (const auto maybeLocation = GetAndUpdateLocation(key, filteredNodeIds)) {
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
    if (!IsReadyPartitionDistribution()) {
        return;
    }
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

void TActorCoordinator::Handle(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
    if (!ev->Get()->Success) {
        LOG_ROW_DISPATCHER_ERROR("Failed to get TEvLookupResult, try later...");
        ScheduleNodeInfoRequest();
        return;
    }
    LOG_ROW_DISPATCHER_INFO("Updated node info, node count: " << ev->Get()->AssignedNodes.size() << ", AssignedNodes: " << JoinSeq(", ", ev->Get()->AssignedNodes));
    NodesCount = ev->Get()->AssignedNodes.size();
    UpdateGlobalState();
    UpdatePendingReadActors();
}

void TActorCoordinator::Handle(TEvPrivate::TEvListNodes::TPtr&) {
    if (NodesManagerId) {
        LOG_ROW_DISPATCHER_DEBUG("Send TEvGetNodesRequest to NodesManager");
        Send(NodesManagerId, new NFq::TEvNodesManager::TEvGetNodesRequest(), IEventHandle::FlagTrackDelivery);
    } else {
        LOG_ROW_DISPATCHER_DEBUG("Send NodeEnumerationLookup request");
        Register(NKikimr::CreateTenantNodeEnumerationLookup(SelfId(), Tenant));
    }
}

void TActorCoordinator::Handle(TEvPrivate::TEvRebalancing::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("Rebalancing...");
    RebalancingScheduled = false;

    bool needRebalance = false;
    TSet<TActorId> toDelete;

    auto printState = [&](const TString& str){
        LOG_ROW_DISPATCHER_DEBUG(str);
        for (auto& [actorId, info] : RowDispatchers) {
            LOG_ROW_DISPATCHER_DEBUG("  node " << actorId.NodeId() << " (" << actorId << ") state " << (info.State == ENodeState::Initializing ? "Initializing" : "Started") << " connected " << info.Connected << " partitions count " << info.Locations.size());
        }
    };

    printState("Current state (rebalancing):");
    
    for (auto& [actorId, info] : RowDispatchers) {
        if (info.State == ENodeState::Initializing) {
            if (info.Connected) {
                info.State = ENodeState::Started;
                needRebalance = true;
            } else {
                toDelete.insert(actorId);
            }
        } else {    // Started
            if (!info.Connected) {
                toDelete.insert(actorId);
                if (!info.Locations.empty()) {
                    needRebalance = true;
                }
            }
        }
    }
    for (const auto& actorId : toDelete) {
        RowDispatchers.erase(actorId);
    }
    if (!needRebalance) {
        return;
    }

    for (const auto& readActorId : KnownReadActors) {
        LOG_ROW_DISPATCHER_TRACE("Send TEvCoordinatorDistributionReset to " << readActorId);
        Send(readActorId, new TEvRowDispatcher::TEvCoordinatorDistributionReset(), IEventHandle::FlagTrackDelivery);
    }

    for (auto& [actorId, info] : RowDispatchers) {
        info.Locations.clear();
    }
    PendingReadActors.clear();
    PartitionLocations.clear();
    TopicsInfo.clear();
    KnownReadActors.clear();

    printState("Current state (after rebalancing):");
}

void TActorCoordinator::Handle(TEvPrivate::TEvStartingTimeout::TPtr&) {
    if (State != ENodeState::Started) {
        LOG_ROW_DISPATCHER_TRACE("Change global state to Started (by timeout)");
        State = ENodeState::Started;
    }
}

bool TActorCoordinator::IsReadyPartitionDistribution() const {
    if (Config.GetLocalMode()) {
        return true;
    }
    return State == ENodeState::Started;
}

void TActorCoordinator::SendError(TActorId readActorId, const TCoordinatorRequest& request, const TString& message) {
    LOG_ROW_DISPATCHER_WARN("Send TEvCoordinatorResult to " << readActorId << ", issues: " << message);
    auto response = std::make_unique<TEvRowDispatcher::TEvCoordinatorResult>();
    NYql::IssuesToMessage(NYql::TIssues{NYql::TIssue{message}}, response->Record.MutableIssues());
    Send(readActorId, response.release(), IEventHandle::FlagTrackDelivery, request.Cookie);
}

void TActorCoordinator::ScheduleNodeInfoRequest() const {
    Schedule(NodesManagerRetryPeriod, new TEvPrivate::TEvListNodes());
}

void TActorCoordinator::Handle(NFq::TEvNodesManager::TEvGetNodesResponse::TPtr& ev) {
    NodesCount = ev->Get()->NodeIds.size();
    LOG_ROW_DISPATCHER_INFO("Updated node info, node count: " << NodesCount);
    UpdateGlobalState();
    if (!NodesCount) {
        ScheduleNodeInfoRequest();
    }
    UpdatePendingReadActors();
}

void TActorCoordinator::UpdateGlobalState() {
     if (State != ENodeState::Started && NodesCount && RowDispatchers.size() >= NodesCount) {
        LOG_ROW_DISPATCHER_TRACE("Change global state to Started (by nodes count)");
        State = ENodeState::Started;
    }
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    NActors::TActorId nodesManagerId)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(rowDispatcherId, config, yqSharedResources, tenant, counters, nodesManagerId));
}

} // namespace NFq
