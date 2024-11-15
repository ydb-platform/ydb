#include "row_dispatcher.h"
#include "common.h"
#include "coordinator.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/public/purecalc/common/interface.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>

#include <util/generic/queue.h>


namespace NFq {

using namespace NActors;

namespace {

const ui64 CoordinatorPingPeriodSec = 2;

////////////////////////////////////////////////////////////////////////////////

struct TRowDispatcherMetrics {
    explicit TRowDispatcherMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        ErrorsCount = Counters->GetCounter("ErrorsCount");
        ClientsCount = Counters->GetCounter("ClientsCount");
        RowsSent = Counters->GetCounter("RowsSent", true);
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ClientsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowsSent;
};


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCoordinatorPing = EvBegin + 20,
        EvUpdateMetrics,
        EvPrintStateToLog,
        EvTryConnect,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvCoordinatorPing : NActors::TEventLocal<TEvCoordinatorPing, EvCoordinatorPing> {};
    struct TEvUpdateMetrics : public NActors::TEventLocal<TEvUpdateMetrics, EvUpdateMetrics> {};
    struct TEvPrintStateToLog : public NActors::TEventLocal<TEvPrintStateToLog, EvPrintStateToLog> {};
    struct TEvTryConnect : public NActors::TEventLocal<TEvTryConnect, EvTryConnect> {
        TEvTryConnect(ui32 nodeId = 0) 
        : NodeId(nodeId) {}
        ui32 NodeId = 0;
    };
};

struct TQueryStat {
    const TString QueryId;
    NYql::TCounters::TEntry UnreadRows;
    NYql::TCounters::TEntry UnreadBytes;
};

ui64 UpdateMetricsPeriodSec = 60;
ui64 PrintStateToLogPeriodSec = 300;

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {

    struct ConsumerSessionKey {
        TActorId ReadActorId;
        ui32 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TActorId>()(ReadActorId);
            hash = CombineHashes<ui64>(hash, std::hash<ui32>()(PartitionId));
            return hash;
        }
        bool operator==(const ConsumerSessionKey& other) const {
            return ReadActorId == other.ReadActorId && PartitionId == other.PartitionId;
        }
    };

    struct ConsumerSessionKeyHash {
        int operator()(const ConsumerSessionKey& k) const {
            return k.Hash();
        }
    };

    struct TopicSessionKey {
        TString Endpoint;
        TString Database;
        TString TopicPath;
        ui64 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(Endpoint);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicPath));
            hash = CombineHashes<ui64>(hash, std::hash<ui64>()(PartitionId));
            return hash;
        }
        bool operator==(const TopicSessionKey& other) const {
            return Endpoint == other.Endpoint && Database == other.Database
                && TopicPath == other.TopicPath && PartitionId == other.PartitionId;
        }
    };

    struct TopicSessionKeyHash {
        int operator()(const TopicSessionKey& k) const {
            return k.Hash();
        }
    };

     struct TNodesTracker{
         class TRetryState {
            public:
                TDuration GetNextDelay() {
                    constexpr TDuration MaxDelay = TDuration::Seconds(10);
                    constexpr TDuration MinDelay = TDuration::MilliSeconds(100); // from second retry
                    TDuration ret = Delay; // The first delay is zero
                    Delay = ClampVal(Delay * 2, MinDelay, MaxDelay);
                    return ret ? RandomizeDelay(ret) : ret;
                }
            private:
                static TDuration RandomizeDelay(TDuration baseDelay) {
                    const TDuration::TValue half = baseDelay.GetValue() / 2;
                    return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
                }
            private:
                TDuration Delay; // The first time retry will be done instantly.
        };

        struct TNodeState {
            bool Connected = false;
            bool RetryScheduled = false;
            TMaybe<TRetryState> RetryState;
        };
    public:
        void Init(const NActors::TActorId& selfId) {
            SelfId = selfId;
        }

        void AddNode(ui32 nodeId) {
            if (Nodes.contains(nodeId)) {
                return;
            }
            HandleNodeDisconnected(nodeId);
        }

        void TryConnect(ui32 nodeId) {
            auto& state = Nodes[nodeId];
            state.RetryScheduled = false;
            if (state.Connected) {
                return;
            }
            auto connectEvent = MakeHolder<NActors::TEvInterconnect::TEvConnectNode>();
            auto proxyId = NActors::TActivationContext::InterconnectProxy(nodeId);
            NActors::TActivationContext::Send(
                new NActors::IEventHandle(proxyId, SelfId, connectEvent.Release(), 0, 0));
        }

        bool GetNodeConnected(ui32 nodeId) {
            return Nodes[nodeId].Connected;
        }

        void HandleNodeConnected(ui32 nodeId) {
            auto& state = Nodes[nodeId];
            state.Connected = true;
            state.RetryState = Nothing();
        }

        void HandleNodeDisconnected(ui32 nodeId) {
            auto& state = Nodes[nodeId];
            state.Connected = false;
            if (state.RetryScheduled) {
                return;
            }
            state.RetryScheduled = true;
            if (!state.RetryState) {
                state.RetryState.ConstructInPlace();
            }
            auto ev = MakeHolder<TEvPrivate::TEvTryConnect>(nodeId);
            auto delay = state.RetryState->GetNextDelay();
            NActors::TActivationContext::Schedule(delay, new NActors::IEventHandle(SelfId, SelfId, ev.Release()));
        }

        void PrintInternalState(TStringStream& stream) const {
            stream << "Nodes states: \n"; 
            for (const auto& [nodeId, state] : Nodes) {
               stream << "  id " << nodeId << " connected " << state.Connected << " retry scheduled " << state.RetryScheduled << "\n";
            }
        }

    private:
        TMap<ui32, TNodeState> Nodes;
        NActors::TActorId SelfId;
        TString LogPrefix = "RowDispatcher: ";
    };


    NConfig::TRowDispatcherConfig Config;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    IPureCalcProgramFactory::TPtr PureCalcProgramFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TMaybe<TActorId> CoordinatorActorId;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const TString LogPrefix;
    ui64 NextEventQueueId = 0;
    TString Tenant;
    NFq::NRowDispatcher::IActorFactory::TPtr ActorFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TRowDispatcherMetrics Metrics;
    NYql::IPqGateway::TPtr PqGateway;
    TNodesTracker NodesTracker;

    struct ConsumerCounters {
        ui64 NewDataArrived = 0;
        ui64 GetNextBatch = 0;
        ui64 MessageBatch = 0;
    };

    struct ConsumerInfo {
        ConsumerInfo(
            NActors::TActorId readActorId,
            NActors::TActorId selfId,
            ui64 eventQueueId,
            NFq::NRowDispatcherProto::TEvStartSession& proto,
            TActorId topicSessionId,
            bool alreadyConnected)
            : ReadActorId(readActorId)
            , SourceParams(proto.GetSource())
            , PartitionId(proto.GetPartitionId())
            , EventQueueId(eventQueueId)
            , Proto(proto)
            , TopicSessionId(topicSessionId)
            , QueryId(proto.GetQueryId()) {
                EventsQueue.Init("txId", selfId, selfId, eventQueueId, /* KeepAlive */ true, /* UseConnect */ false);
                EventsQueue.OnNewRecipientId(readActorId, true, alreadyConnected);
            }

        NActors::TActorId ReadActorId;
        NYql::NPq::NProto::TDqPqTopicSource SourceParams;
        ui64 PartitionId;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        ui64 EventQueueId;
        NFq::NRowDispatcherProto::TEvStartSession Proto;
        TActorId TopicSessionId;
        const TString QueryId;
        ConsumerCounters Counters;
        bool PendingGetNextBatch = false;
        bool PendingNewDataArrived = false;
        TopicSessionClientStatistic Stat;
    };

    struct SessionInfo {
        TMap<TActorId, TAtomicSharedPtr<ConsumerInfo>> Consumers;     // key - ReadActor actor id
        TopicSessionCommonStatistic Stat;
    };

    struct TopicSessionInfo {
        TMap<TActorId, SessionInfo> Sessions;                         // key - TopicSession actor id
    };

    THashMap<ConsumerSessionKey, TAtomicSharedPtr<ConsumerInfo>, ConsumerSessionKeyHash> Consumers;
    TMap<ui64, TAtomicSharedPtr<ConsumerInfo>> ConsumersByEventQueueId;
    THashMap<TopicSessionKey, TopicSessionInfo, TopicSessionKeyHash> TopicSessions;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& tenant,
        const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway);

    void Bootstrap();

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER";

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) ;
    void Handle(TEvPrivate::TEvCoordinatorPing::TPtr& ev);
    void Handle(NActors::TEvents::TEvPong::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionStatistic::TPtr& ev);

    void Handle(NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev);
    void Handle(const TEvPrivate::TEvTryConnect::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr&);
    void Handle(NFq::TEvPrivate::TEvUpdateMetrics::TPtr&);
    void Handle(NFq::TEvPrivate::TEvPrintStateToLog::TPtr&);
    void Handle(const NMon::TEvHttpInfo::TPtr&);
    
    void DeleteConsumer(const ConsumerSessionKey& key);
    void UpdateMetrics();
    TString GetInternalState();

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvPrivate::TEvCoordinatorPing, Handle)
        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStatus, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionStatistic, Handle);
        hFunc(TEvPrivate::TEvTryConnect, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvPrivate::TEvUpdateMetrics, Handle);
        hFunc(NFq::TEvPrivate::TEvPrintStateToLog, Handle);
        hFunc(NMon::TEvHttpInfo, Handle);
    })
};

TRowDispatcher::TRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , PureCalcProgramFactory(CreatePureCalcProgramFactory())
    , YqSharedResources(yqSharedResources)
    , CredentialsFactory(credentialsFactory)
    , LogPrefix("RowDispatcher: ")
    , Tenant(tenant)
    , ActorFactory(actorFactory)
    , Counters(counters)
    , Metrics(counters)
    , PqGateway(pqGateway) {
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId() << ", tenant " << Tenant);

    const auto& config = Config.GetCoordinator();
    auto coordinatorId = Register(NewCoordinator(SelfId(), config, YqSharedResources, Tenant, Counters).release());
    Register(NewLeaderElection(SelfId(), coordinatorId, config, CredentialsProviderFactory, YqSharedResources, Tenant, Counters).release());
    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    Schedule(TDuration::Seconds(PrintStateToLogPeriodSec), new NFq::TEvPrivate::TEvPrintStateToLog());

    NActors::TMon* mon = NKikimr::AppData()->Mon;
    if (mon) {
        ::NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "row_dispatcher", "Row Dispatcher", false,
            TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }
    NodesTracker.Init(SelfId());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Coordinator changed, old leader " << CoordinatorActorId << ", new " << ev->Get()->CoordinatorActorId);

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery);
    for (auto actorId : CoordinatorChangedSubscribers) {
        Send(
            actorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorChanged(ev->Get()->CoordinatorActorId),
            IEventHandle::FlagTrackDelivery);
    }
}

void TRowDispatcher::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected, node id " << ev->Get()->NodeId);
    NodesTracker.HandleNodeConnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    NodesTracker.HandleNodeDisconnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString() << ", reason " << ev->Get()->Reason);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleUndelivered(ev);
    }
}

void TRowDispatcher::Handle(TEvPrivate::TEvCoordinatorPing::TPtr&) {
    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    if (!CoordinatorActorId) {
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Send ping to " << *CoordinatorActorId);
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing());
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPong::TPtr&) {
    LOG_ROW_DISPATCHER_TRACE("NActors::TEvents::TEvPong");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChangesSubscribe from " << ev->Sender);
    NodesTracker.AddNode(ev->Sender.NodeId());
    CoordinatorChangedSubscribers.insert(ev->Sender);
    if (!CoordinatorActorId) {
        return;
    }
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId), IEventHandle::FlagTrackDelivery);
}

void TRowDispatcher::UpdateMetrics() {
    if (Consumers.empty()) {
        return;
    }
    TMap<TString, TQueryStat> queryStats;
    
    for (auto& [key, sessionsInfo] : TopicSessions) {
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                auto& stat = queryStats[consumer->QueryId];
                stat.UnreadRows.Add(NYql::TCounters::TEntry(consumer->Stat.UnreadRows));
                stat.UnreadBytes.Add(NYql::TCounters::TEntry(consumer->Stat.UnreadBytes));
            }
        }
    }
    for (const auto& [queryId, stat] : queryStats) {
        auto queryGroup = Metrics.Counters->GetSubgroup("queryId", queryId);
        queryGroup->GetCounter("MaxUnreadRows")->Set(stat.UnreadRows.Max);
        queryGroup->GetCounter("AvgUnreadRows")->Set(stat.UnreadRows.Avg);
        queryGroup->GetCounter("MaxUnreadBytes")->Set(stat.UnreadBytes.Max);
        queryGroup->GetCounter("AvgUnreadBytes")->Set(stat.UnreadBytes.Avg);
    }
}

TString TRowDispatcher::GetInternalState() {
    TStringStream str;
    NodesTracker.PrintInternalState(str);
    str << "Statistics:\n";
    for (auto& [key, sessionsInfo] : TopicSessions) {
        str << "  " << key.Endpoint << " / " << key.Database << " / " << key.TopicPath << " / " << key.PartitionId;
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            str << " / " << actorId << "\n";
            str << "    unread bytes " << sessionInfo.Stat.UnreadBytes << " restarts by offsets " << sessionInfo.Stat.RestartSessionByOffsets << "\n";
            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                str << "    " << consumer->QueryId << " " << readActorId << " unread rows "
                    << consumer->Stat.UnreadRows << " unread bytes " << consumer->Stat.UnreadBytes << " offset " << consumer->Stat.Offset
                    << " get " << consumer->Counters.GetNextBatch
                    << " arr " << consumer->Counters.NewDataArrived << " btc " << consumer->Counters.MessageBatch 
                    << " pend get " <<  consumer->PendingGetNextBatch << " pend new " <<  consumer->PendingNewDataArrived << " ";
                str << " retry queue: ";
                consumer->EventsQueue.PrintInternalState(str);
            }
        }
    }
    return str.Str();
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStartSession from " << ev->Sender << ", topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());
    NodesTracker.AddNode(ev->Sender.NodeId());
    TMaybe<ui64> readOffset;
    if (ev->Get()->Record.HasOffset()) {
        readOffset = ev->Get()->Record.GetOffset();
    }

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it != Consumers.end()) {
        LOG_ROW_DISPATCHER_ERROR("Consumer already exists, ignore StartSession");
        return;
    }
    const auto& source = ev->Get()->Record.GetSource();

    TActorId sessionActorId;
    TopicSessionKey topicKey{source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
    LOG_ROW_DISPATCHER_DEBUG("Topic session count " << topicSessionInfo.Sessions.size());
    Y_ENSURE(topicSessionInfo.Sessions.size() <= 1);

    auto consumerInfo = MakeAtomicShared<ConsumerInfo>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record, TActorId(), NodesTracker.GetNodeConnected(ev->Sender.NodeId()));
    Consumers[key] = consumerInfo;
    ConsumersByEventQueueId[consumerInfo->EventQueueId] = consumerInfo;
    if (!consumerInfo->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();
        LOG_ROW_DISPATCHER_ERROR("TEvStartSession: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
    }

    if (topicSessionInfo.Sessions.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Create new session " << readOffset);
        sessionActorId = ActorFactory->RegisterTopicSession(
            source.GetTopicPath(),
            source.GetEndpoint(),
            source.GetDatabase(),
            Config,
            SelfId(),
            ev->Get()->Record.GetPartitionId(),
            YqSharedResources->UserSpaceYdbDriver,
            CreateCredentialsProviderFactoryForStructuredToken(
                CredentialsFactory,
                ev->Get()->Record.GetToken(),
                source.GetAddBearerToToken()),
            PureCalcProgramFactory,
            Counters,
            PqGateway
            );
        SessionInfo& sessionInfo = topicSessionInfo.Sessions[sessionActorId];
        sessionInfo.Consumers[ev->Sender] = consumerInfo;
    } else {
        auto sessionIt = topicSessionInfo.Sessions.begin();
        SessionInfo& sessionInfo = sessionIt->second;
        sessionInfo.Consumers[ev->Sender] = consumerInfo;
        sessionActorId = sessionIt->first;
    }
    consumerInfo->TopicSessionId = sessionActorId;
    consumerInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvStartSessionAck(consumerInfo->Proto));

    Forward(ev, sessionActorId);
    Metrics.ClientsCount->Set(Consumers.size());
    UpdateMetrics();
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    LOG_ROW_DISPATCHER_TRACE("TEvGetNextBatch from " << ev->Sender << ", partId " << ev->Get()->Record.GetPartitionId() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvGetNextBatch, no such session");
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();
        LOG_ROW_DISPATCHER_ERROR("TEvGetNextBatch: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
        return;
    }
    it->second->PendingNewDataArrived = false;
    it->second->PendingGetNextBatch = true;
    it->second->Counters.GetNextBatch++;
    Forward(ev, it->second->TopicSessionId);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvHeartbeat from " << ev->Sender);
    
    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Wrong consumer, sender " << ev->Sender << ", part id " << ev->Cookie);
        return;
    }
    it->second->EventsQueue.OnEventReceived(ev);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());
    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Wrong consumer, sender " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();

        LOG_ROW_DISPATCHER_ERROR("TEvStopSession: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
        return;
    }
    DeleteConsumer(key);
}

void TRowDispatcher::DeleteConsumer(const ConsumerSessionKey& key) {
    LOG_ROW_DISPATCHER_DEBUG("DeleteConsumer, readActorId " << key.ReadActorId <<
        " partitionId " << key.PartitionId);

    auto consumerIt = Consumers.find(key);
    if (consumerIt == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore DeleteConsumer, no such session");
        return;
    }
    const auto& consumer = consumerIt->second;
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    *event->Record.MutableSource() = consumer->SourceParams;
    event->Record.SetPartitionId(consumer->PartitionId);
    Send(new IEventHandle(consumerIt->second->TopicSessionId, consumer->ReadActorId, event.release(), 0));

    TopicSessionKey topicKey{
        consumer->SourceParams.GetEndpoint(),
        consumer->SourceParams.GetDatabase(),
        consumer->SourceParams.GetTopicPath(),
        consumer->PartitionId};
    TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
    SessionInfo& sessionInfo = topicSessionInfo.Sessions[consumerIt->second->TopicSessionId];
    Y_ENSURE(sessionInfo.Consumers.count(consumer->ReadActorId));
    sessionInfo.Consumers.erase(consumer->ReadActorId);
    if (sessionInfo.Consumers.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Session is not used, sent TEvPoisonPill");
        topicSessionInfo.Sessions.erase(consumerIt->second->TopicSessionId);
        Send(consumerIt->second->TopicSessionId, new NActors::TEvents::TEvPoisonPill());
        if (topicSessionInfo.Sessions.empty()) {
            TopicSessions.erase(topicKey);
        }
    }
    ConsumersByEventQueueId.erase(consumerIt->second->EventQueueId);
    Consumers.erase(consumerIt);
    Metrics.ClientsCount->Set(Consumers.size());
    UpdateMetrics();
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr& ev) {
    LOG_ROW_DISPATCHER_WARN("Session closed, event queue id " << ev->Get()->EventQueueId);
    for (auto& [consumerKey, consumer] : Consumers) {
        if (consumer->EventQueueId != ev->Get()->EventQueueId) {
            continue;
        }
        DeleteConsumer(consumerKey);
        break;
    }
}

void TRowDispatcher::Handle(const TEvPrivate::TEvTryConnect::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvTryConnect to node id " << ev->Get()->NodeId);
    NodesTracker.TryConnect(ev->Get()->NodeId);
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvRetryQueuePrivate::TEvEvHeartbeat " << ev->Get()->EventQueueId);
    auto it = ConsumersByEventQueueId.find(ev->Get()->EventQueueId);
    if (it == ConsumersByEventQueueId.end()) {
        LOG_ROW_DISPATCHER_WARN("No consumer with EventQueueId = " << ev->Get()->EventQueueId);
        return;
    }
    auto& sessionInfo = it->second;

    bool needSend = sessionInfo->EventsQueue.Heartbeat();
    if (needSend) {
        sessionInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(sessionInfo->PartitionId));
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {    
    LOG_ROW_DISPATCHER_TRACE("TEvNewDataArrived from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvNewDataArrived, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvNewDataArrived to " << ev->Get()->ReadActorId);
    it->second->PendingNewDataArrived = true;
    it->second->Counters.NewDataArrived++;
    it->second->EventsQueue.Send(ev->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvMessageBatch from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    Metrics.RowsSent->Add(ev->Get()->Record.MessagesSize());
    LOG_ROW_DISPATCHER_TRACE("Forward TEvMessageBatch to " << ev->Get()->ReadActorId);
    it->second->PendingGetNextBatch = false;
    it->second->Counters.MessageBatch++;
    it->second->EventsQueue.Send(ev->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvSessionError from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    Metrics.ErrorsCount->Inc();
    LOG_ROW_DISPATCHER_TRACE("Forward TEvSessionError to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev->Release().Release());
    DeleteConsumer(key);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvStatus from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvStatus, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvStatus to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvUpdateMetrics::TPtr&) {
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    UpdateMetrics();
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvPrintStateToLog::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG(GetInternalState());
    Schedule(TDuration::Seconds(PrintStateToLogPeriodSec), new NFq::TEvPrivate::TEvPrintStateToLog());
}

void TRowDispatcher::Handle(const NMon::TEvHttpInfo::TPtr& ev) {
    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Current state:" << Endl;
            str << GetInternalState() << Endl;
            str << Endl;
        }
    }
    Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionStatistic::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvSessionStatistic from " << ev->Sender);
    const auto& key = ev->Get()->Stat.SessionKey;
    TopicSessionKey sessionKey{key.Endpoint, key.Database, key.TopicPath, key.PartitionId};

    auto sessionsIt = TopicSessions.find(sessionKey);
    if (sessionsIt == TopicSessions.end()) {
        return;
    }
    auto& sessionsInfo = sessionsIt->second;
    auto sessionIt = sessionsInfo.Sessions.find(ev->Sender);
    if (sessionIt == sessionsInfo.Sessions.end()) {
        return;
    }

    auto& sessionInfo = sessionIt->second;
    sessionInfo.Stat = ev->Get()->Stat.Common;

    for (const auto& clientStat : ev->Get()->Stat.Clients) {
        auto it = sessionInfo.Consumers.find(clientStat.ReadActorId);
        if (it == sessionInfo.Consumers.end()) {
            continue;
        }
        auto consumerInfoPtr = it->second; 
        consumerInfoPtr->Stat = clientStat;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
        config,
        credentialsProviderFactory,
        yqSharedResources,
        credentialsFactory,
        tenant,
        actorFactory,
        counters,
        pqGateway));
}

} // namespace NFq
