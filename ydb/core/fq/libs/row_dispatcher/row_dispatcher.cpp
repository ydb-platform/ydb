#include "row_dispatcher.h"

#include "actors_factory.h"
#include "common.h"
#include "coordinator.h"
#include "leader_election.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <yql/essentials/public/purecalc/common/interface.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <util/generic/queue.h>
#include <util/stream/format.h>

namespace NFq {

using namespace NActors;

namespace {

const ui64 CoordinatorPingPeriodSec = 2;

////////////////////////////////////////////////////////////////////////////////

struct TRowDispatcherMetrics {
    explicit TRowDispatcherMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        ErrorsCount = Counters->GetCounter("ErrorsCount", true);
        ClientsCount = Counters->GetCounter("ClientsCount");
        RowsSent = Counters->GetCounter("RowsSent", true);
        NodesReconnect = Counters->GetCounter("NodesReconnect", true);
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ClientsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowsSent;
    ::NMonitoring::TDynamicCounters::TCounterPtr NodesReconnect;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCoordinatorPing = EvBegin + 20,
        EvUpdateMetrics,
        EvPrintStateToLog,
        EvTryConnect,
        EvSendStatistic,
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
    struct TEvSendStatistic : public NActors::TEventLocal<TEvSendStatistic, EvSendStatistic> {};
};

struct TQueryStatKey {
    TString QueryId;
    TString ReadGroup;

    size_t Hash() const noexcept {
        ui64 hash = std::hash<TString>()(QueryId);
        hash = CombineHashes<ui64>(hash, std::hash<TString>()(ReadGroup));
        return hash;
    }
    bool operator==(const TQueryStatKey& other) const {
        return QueryId == other.QueryId && ReadGroup == other.ReadGroup;
    }
};

struct TQueryStatKeyHash {
    size_t operator()(const TQueryStatKey& k) const {
        return k.Hash();
    }
};

struct TAggQueryStat {
    NYql::TCounters::TEntry FilteredReadBytes;
    NYql::TCounters::TEntry UnreadBytes;
    NYql::TCounters::TEntry UnreadRows;
    NYql::TCounters::TEntry ReadLagMessages;
    bool IsWaiting = false;

    void Add(const TopicSessionClientStatistic& stat) {
        FilteredReadBytes.Add(NYql::TCounters::TEntry(stat.FilteredReadBytes));
        UnreadBytes.Add(NYql::TCounters::TEntry(stat.UnreadBytes));
        UnreadRows.Add(NYql::TCounters::TEntry(stat.UnreadRows));
        ReadLagMessages.Add(NYql::TCounters::TEntry(stat.ReadLagMessages));
        IsWaiting = IsWaiting || stat.IsWaiting;
    }        
};

ui64 UpdateMetricsPeriodSec = 60;
ui64 PrintStateToLogPeriodSec = 600;
ui64 PrintStateToLogSplitSize = 512000;
ui64 MaxSessionBufferSizeBytes = 16000000;

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {

    struct TopicSessionKey {
        TString ReadGroup;
        TString Endpoint;
        TString Database;
        TString TopicPath;
        ui64 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(ReadGroup);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Endpoint));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicPath));
            hash = CombineHashes<ui64>(hash, std::hash<ui64>()(PartitionId));
            return hash;
        }
        bool operator==(const TopicSessionKey& other) const {
            return ReadGroup == other.ReadGroup && Endpoint == other.Endpoint && Database == other.Database
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

        struct TCounters {
            ui64 Connected = 0;
            ui64 Disconnected = 0;
        };

        struct TNodeState {
            bool Connected = false;
            bool RetryScheduled = false;
            TMaybe<TRetryState> RetryState;
            TCounters Counters;
        };
    public:
        void Init(const NActors::TActorId& selfId) {
            SelfId = selfId;
        }

        void AddNode(ui32 nodeId) {
            if (Nodes.contains(nodeId)) {
                return;
            }
            if (nodeId == SelfId.NodeId()) {
                HandleNodeConnected(nodeId);      // always сconnected
            } else {
                HandleNodeDisconnected(nodeId);
            }
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
            state.Counters.Connected++;
        }

        void HandleNodeDisconnected(ui32 nodeId) {
            auto& state = Nodes[nodeId];
            state.Connected = false;
            state.Counters.Disconnected++;
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
                stream << "  id " << nodeId << " connected " << state.Connected << " retry scheduled " << state.RetryScheduled
                    << " connected count " << state.Counters.Connected << " disconnected count " << state.Counters.Disconnected << "\n";
            }
        }

    private:
        TMap<ui32, TNodeState> Nodes;
        NActors::TActorId SelfId;
        TString LogPrefix = "RowDispatcher: ";
    };

    struct TAggregatedStats{
        NYql::TCounters::TEntry AllSessionsReadBytes;
        THashMap<TQueryStatKey, TMaybe<TAggQueryStat>, TQueryStatKeyHash> LastQueryStats;
        TDuration LastUpdateMetricsPeriod;
    };

    NConfig::TRowDispatcherConfig Config;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId CompileServiceActorId;
    TMaybe<TActorId> CoordinatorActorId;
    ui64 CoordinatorGeneration = 0;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const TString LogPrefix;
    ui64 NextEventQueueId = 0;
    TString Tenant;
    NFq::NRowDispatcher::IActorFactory::TPtr ActorFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TRowDispatcherMetrics Metrics;
    NYql::IPqGateway::TPtr PqGateway;
    NActors::TMon* Monitoring;
    TNodesTracker NodesTracker;
    TAggregatedStats AggrStats; 

    struct ConsumerCounters {
        ui64 NewDataArrived = 0;
        ui64 GetNextBatch = 0;
        ui64 MessageBatch = 0;
    };

    struct TConsumerPartition {
        bool PendingGetNextBatch = false;
        bool PendingNewDataArrived = false;
        TActorId TopicSessionId;
        TopicSessionClientStatistic Stat;
        bool StatisticsUpdated = false;
    };

    struct ConsumerInfo {
        ConsumerInfo(
            NActors::TActorId readActorId,
            NActors::TActorId selfId,
            ui64 eventQueueId,
            NFq::NRowDispatcherProto::TEvStartSession& proto,
            bool alreadyConnected,
            ui64 generation)
            : ReadActorId(readActorId)
            , SourceParams(proto.GetSource())
            , EventQueueId(eventQueueId)
            , Proto(proto)
            , QueryId(proto.GetQueryId())
            , Generation(generation) {
                EventsQueue.Init("txId", selfId, selfId, eventQueueId, /* KeepAlive */ true, /* UseConnect */ false);
                EventsQueue.OnNewRecipientId(readActorId, true, alreadyConnected);
            }

        NActors::TActorId ReadActorId;
        NYql::NPq::NProto::TDqPqTopicSource SourceParams;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        ui64 EventQueueId;
        NFq::NRowDispatcherProto::TEvStartSession Proto;
        THashMap<ui32, TConsumerPartition> Partitions;
        const TString QueryId;
        ConsumerCounters Counters;
        TopicSessionClientStatistic Stat;
        ui64 Generation;
    };

    struct SessionInfo {
        TMap<TActorId, TAtomicSharedPtr<ConsumerInfo>> Consumers;   // key - ReadActor actor id
        TopicSessionCommonStatistic Stat;                           // Increments
        NYql::TCounters::TEntry AggrReadBytes;
    };

    struct TopicSessionInfo {
        TMap<TActorId, SessionInfo> Sessions;                         // key - TopicSession actor id
    };

    struct ReadActorInfo {
        TString InternalState;
        TInstant RequestTime;
        TInstant ResponseTime;
    };

    THashMap<NActors::TActorId, TAtomicSharedPtr<ConsumerInfo>> Consumers;      // key - read actor id
    TMap<ui64, TAtomicSharedPtr<ConsumerInfo>> ConsumersByEventQueueId;
    THashMap<TopicSessionKey, TopicSessionInfo, TopicSessionKeyHash> TopicSessions;
    TMap<TActorId, ReadActorInfo> ReadActorsInternalState;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& tenant,
        const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway,
        NActors::TMon* monitoring = nullptr);

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
    void Handle(NFq::TEvRowDispatcher::TEvSessionStatistic::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetInternalStateResponse::TPtr& ev);

    void Handle(NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev);
    void Handle(const TEvPrivate::TEvTryConnect::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr&);
    void Handle(NFq::TEvPrivate::TEvUpdateMetrics::TPtr&);
    void Handle(NFq::TEvPrivate::TEvPrintStateToLog::TPtr&);
    void Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&);
    void Handle(const NMon::TEvHttpInfo::TPtr&);
    
    void DeleteConsumer(NActors::TActorId readActorId);
    void UpdateMetrics();
    TString GetInternalState();
    TString GetReadActorsInternalState();
    void UpdateReadActorsInternalState();
    template <class TEventPtr>
    bool CheckSession(TAtomicSharedPtr<ConsumerInfo>& consumer, const TEventPtr& ev);
    void SetQueryMetrics(const TQueryStatKey& queryKey, ui64 unreadBytesMax, ui64 unreadBytesAvg, i64 readLagMessagesMax);
    void PrintStateToLog();

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
        hFunc(NFq::TEvRowDispatcher::TEvSessionStatistic, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetInternalStateResponse, Handle);
        hFunc(TEvPrivate::TEvTryConnect, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvPrivate::TEvUpdateMetrics, Handle);
        hFunc(NFq::TEvPrivate::TEvPrintStateToLog, Handle);
        hFunc(NFq::TEvPrivate::TEvSendStatistic, Handle);
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
    const NYql::IPqGateway::TPtr& pqGateway,
    NActors::TMon* monitoring)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , CredentialsFactory(credentialsFactory)
    , LogPrefix("RowDispatcher: ")
    , Tenant(tenant)
    , ActorFactory(actorFactory)
    , Counters(counters)
    , Metrics(counters)
    , PqGateway(pqGateway)
    , Monitoring(monitoring)
{
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId() << ", tenant " << Tenant);

    const auto& config = Config.GetCoordinator();
    auto coordinatorId = Register(NewCoordinator(SelfId(), config, YqSharedResources, Tenant, Counters).release());
    Register(NewLeaderElection(SelfId(), coordinatorId, config, CredentialsProviderFactory, YqSharedResources, Tenant, Counters).release());

    CompileServiceActorId = Register(NRowDispatcher::CreatePurecalcCompileService());

    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    Schedule(TDuration::Seconds(PrintStateToLogPeriodSec), new NFq::TEvPrivate::TEvPrintStateToLog());
    Schedule(TDuration::Seconds(Config.GetSendStatusPeriodSec()), new NFq::TEvPrivate::TEvSendStatistic());

    if (Monitoring) {
        ::NMonitoring::TIndexMonPage* actorsMonPage = Monitoring->RegisterIndexPage("actors", "Actors");
        Monitoring->RegisterActorPage(actorsMonPage, "row_dispatcher", "Row Dispatcher", false,
            TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }
    NodesTracker.Init(SelfId());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Coordinator changed, old leader " << CoordinatorActorId << ", new " << ev->Get()->CoordinatorActorId << " generation " << ev->Get()->Generation);
    if (ev->Get()->Generation < CoordinatorGeneration) {
        LOG_ROW_DISPATCHER_ERROR("New generation (" << ev->Get()->Generation << ") is less previous (" << CoordinatorGeneration << "), ignore updates");
        return;
    }
    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    CoordinatorGeneration = ev->Get()->Generation;
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery);
    for (auto actorId : CoordinatorChangedSubscribers) {
        Send(
            actorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId, CoordinatorGeneration),
            IEventHandle::FlagTrackDelivery);
    }
}

void TRowDispatcher::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected, node id " << ev->Get()->NodeId);
    Metrics.NodesReconnect->Inc();
    NodesTracker.HandleNodeConnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    Metrics.NodesReconnect->Inc();
    NodesTracker.HandleNodeDisconnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvUndelivered, from " << ev->Sender << ", reason " << ev->Get()->Reason);
    for (auto& [key, consumer] : Consumers) {
        if (ev->Cookie != consumer->Generation) {       // Several partitions in one read_actor have different Generation.
            continue;
        }
        if (consumer->EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            DeleteConsumer(ev->Sender);
            break;
        }
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
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId, CoordinatorGeneration), IEventHandle::FlagTrackDelivery);
}

void TRowDispatcher::UpdateMetrics() {
    static TInstant LastUpdateMetricsTime = TInstant::Now();
    auto now = TInstant::Now();
    AggrStats.LastUpdateMetricsPeriod = now - LastUpdateMetricsTime;
    LastUpdateMetricsTime = now;
    auto secs = AggrStats.LastUpdateMetricsPeriod.Seconds();
    if (!secs) {
        return;
    }

    AggrStats.AllSessionsReadBytes = NYql::TCounters::TEntry();
    for (auto& [queryId, stat] : AggrStats.LastQueryStats) {
        stat = Nothing();
    }

    for (auto& [key, sessionsInfo] : TopicSessions) {
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            auto read = NYql::TCounters::TEntry(sessionInfo.Stat.ReadBytes);
            AggrStats.AllSessionsReadBytes.Add(read);
            sessionInfo.AggrReadBytes = read;
            sessionInfo.Stat.Clear();

            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                auto& stat = AggrStats.LastQueryStats[TQueryStatKey{consumer->QueryId, key.ReadGroup}];
                if (!stat) {
                    stat = TAggQueryStat();
                }
                stat->Add(consumer->Stat);
                consumer->Stat.Clear();
            }
        }
    }
    THashSet<TQueryStatKey, TQueryStatKeyHash> toDelete;
    for (const auto& [key, stats] : AggrStats.LastQueryStats) {
        if (!stats) {
            toDelete.insert(key);
            continue;
        }
        SetQueryMetrics(key, stats->UnreadBytes.Max, stats->UnreadBytes.Avg, stats->ReadLagMessages.Max);
    }
    for (const auto& key : toDelete) {
         SetQueryMetrics(key, 0, 0, 0);
         AggrStats.LastQueryStats.erase(key);
    }
    PrintStateToLog();
}

void TRowDispatcher::SetQueryMetrics(const TQueryStatKey& queryKey, ui64 unreadBytesMax, ui64 unreadBytesAvg, i64 readLagMessagesMax) {
    auto queryGroup = Metrics.Counters->GetSubgroup("query_id", queryKey.QueryId);
    auto topicGroup = queryGroup->GetSubgroup("read_group", CleanupCounterValueString(queryKey.ReadGroup));
    topicGroup->GetCounter("MaxUnreadBytes")->Set(unreadBytesMax);
    topicGroup->GetCounter("AvgUnreadBytes")->Set(unreadBytesAvg);
    topicGroup->GetCounter("MaxReadLag")->Set(readLagMessagesMax);
}

TString TRowDispatcher::GetInternalState() {
    TStringStream str;
    NodesTracker.PrintInternalState(str);
    auto secs = AggrStats.LastUpdateMetricsPeriod.Seconds();
    if (!secs) {
        str << "LastUpdatePeriod is null!" << "\n";
        secs += 1;
    }
    auto leftPad = [](auto value) {
        return LeftPad(value, 10);
    };

    auto toHuman = [&](ui64 value) {
        return leftPad(HumanReadableSize(value, SF_BYTES));
    };

    auto toHumanDR = [&](ui64 value) {
        return leftPad(toHuman(value / secs));
    };

    auto printDataRate = [&](NYql::TCounters::TEntry entry) {
        str << " (sum " << toHumanDR(entry.Sum) << "   max  " << toHumanDR(entry.Max) << "   min " << toHumanDR(entry.Min) << ")";
    };
    str << "SelfId: " << SelfId().ToString() << "\n";
    str << "Consumers count: " << Consumers.size() << "\n";
    str << "TopicSessions count: " << TopicSessions.size() << "\n";
    str << "Max session buffer size: " << toHuman(MaxSessionBufferSizeBytes) << "\n";
    str << "DataRate (all sessions): ";
    printDataRate(AggrStats.AllSessionsReadBytes);
    str << "\n";

    THashMap<TQueryStatKey, TAggQueryStat, TQueryStatKeyHash> queryState;
    THashMap<TQueryStatKey, ui64, TQueryStatKeyHash> sessionCountByQuery;
    ui64 unreadBytesSum = 0;

    for (auto& [sessionKey, sessionsInfo] : TopicSessions) {
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            unreadBytesSum += sessionInfo.Stat.UnreadBytes;
            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                auto key = TQueryStatKey{consumer->QueryId, sessionKey.ReadGroup};
                ++sessionCountByQuery[key];
                queryState[key].Add(consumer->Stat);
            }
        }
    }

    if (TopicSessions.size()) {
        str << "Buffer used: " <<  Prec(unreadBytesSum * 100.0 / (TopicSessions.size() * MaxSessionBufferSizeBytes), 4) << "% (" << toHuman(unreadBytesSum) << ")\n";
    }

    str << "Queries:\n";
    for (const auto& [queryStatKey, stat]: queryState) {
        auto [queryId, readGroup] = queryStatKey;
        const auto& aggStat = AggrStats.LastQueryStats[queryStatKey];
        auto sessionsBufferSumSize = sessionCountByQuery[queryStatKey] * MaxSessionBufferSizeBytes;
        auto used = sessionsBufferSumSize ? (stat.UnreadBytes.Sum * 100.0 / sessionsBufferSumSize) : 0.0;
        str << "  " << queryId << " / " << readGroup << ": buffer used (all partitions) " << LeftPad(Prec(used, 4), 10) << "% (" << toHuman(stat.UnreadBytes.Sum) <<  ") unread max (one partition) " << toHuman(stat.UnreadBytes.Max) << " data rate";
        if (aggStat) {
            printDataRate(aggStat->FilteredReadBytes);
        }
        str << " waiting " << stat.IsWaiting << " max read lag " << stat.ReadLagMessages.Max;
        str << "\n";
    }
    str << "TopicSessions:\n";
    for (auto& [key, sessionsInfo] : TopicSessions) {
        str << "  " << key.TopicPath << " / " << key.PartitionId  << " / " << key.ReadGroup;
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            str << " / " << LeftPad(actorId, 32)
                << " data rate " << toHumanDR(sessionInfo.AggrReadBytes.Sum) << " unread bytes " << toHuman(sessionInfo.Stat.UnreadBytes)
                << " offset " << LeftPad(sessionInfo.Stat.LastReadedOffset, 12) << " restarts by offsets " << sessionInfo.Stat.RestartSessionByOffsets
                << " parse and filter lantecy " << sessionInfo.Stat.ParseAndFilterLatency << "\n";
            ui64 maxInitialOffset = 0;
            ui64 minInitialOffset = std::numeric_limits<ui64>::max();

            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                const auto& partition = consumer->Partitions[key.PartitionId];
                str << "    " << consumer->QueryId << " " << LeftPad(readActorId, 32) << " unread bytes "
                    << toHuman(consumer->Stat.UnreadBytes) << " (" << leftPad(consumer->Stat.UnreadRows) << " rows) "
                    << " offset " << leftPad(consumer->Stat.Offset) << " init offset " << leftPad(consumer->Stat.InitialOffset)
                    << " get " << leftPad(consumer->Counters.GetNextBatch)
                    << " arr " << leftPad(consumer->Counters.NewDataArrived) << " btc " << leftPad(consumer->Counters.MessageBatch) 
                    << " pend get " <<  leftPad(partition.PendingGetNextBatch) << " pend new " << leftPad(partition.PendingNewDataArrived)
                    << " waiting " <<  consumer->Stat.IsWaiting << " read lag " << leftPad(consumer->Stat.ReadLagMessages) 
                    << " conn id " <<  consumer->Generation << "\n";
                maxInitialOffset = std::max(maxInitialOffset, consumer->Stat.InitialOffset);
                minInitialOffset = std::min(minInitialOffset, consumer->Stat.InitialOffset);
            }
            str << "    initial offset max " << leftPad(maxInitialOffset) << " min " << leftPad(minInitialOffset) << "\n";;
        }
    }

    str << "Consumers:\n";
    for (auto& [readActorId, consumer] : Consumers) {
        str << "  " << consumer->QueryId << " " << LeftPad(readActorId, 32) << " Generation " << consumer->Generation <<  "\n";
        str << "    partitions: "; 
        for (const auto& [partitionId, info] : consumer->Partitions) {
            str << partitionId << ","; 
        }
        str << "\n    retry queue: ";
        consumer->EventsQueue.PrintInternalState(str);
    }

    return str.Str();
}

TString TRowDispatcher::GetReadActorsInternalState() {
    TStringStream str;
    for (const auto& [_, internalState]: ReadActorsInternalState) {
        str << "ResponseTime: " << internalState.ResponseTime << " " << internalState.InternalState << Endl;
    }
    return str.Str();
}

void TRowDispatcher::UpdateReadActorsInternalState() {
    TSet<TActorId> ReadActors;
    for (const auto& [readActorId, _]: Consumers) {
        ReadActors.insert(readActorId);
    }

    for(auto it = ReadActorsInternalState.begin(); it != ReadActorsInternalState.end();) {
        if (!ReadActors.contains(it->first)) {
            it = ReadActorsInternalState.erase(it);
        } else {
            ++it;
        }
    }

    auto now = TInstant::Now();
    for (const auto& readActor: ReadActors) {
        auto& internalStateInfo = ReadActorsInternalState[readActor];
        if (now - internalStateInfo.RequestTime < TDuration::Seconds(30)) {
            continue;
        }
        internalStateInfo.RequestTime = now;
        Send(readActor, new NFq::TEvRowDispatcher::TEvGetInternalStateRequest{}, 0, 0);
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Received TEvStartSession from " << ev->Sender << ", read group " << ev->Get()->Record.GetSource().GetReadGroup() << ", topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " part id " << JoinSeq(',', ev->Get()->Record.GetPartitionId()) << " query id " << ev->Get()->Record.GetQueryId() << " cookie " << ev->Cookie);
    auto queryGroup = Metrics.Counters->GetSubgroup("query_id", ev->Get()->Record.GetQueryId());
    auto topicGroup = queryGroup->GetSubgroup("read_group", CleanupCounterValueString(ev->Get()->Record.GetSource().GetReadGroup()));
    topicGroup->GetCounter("StartSession", true)->Inc();

    NodesTracker.AddNode(ev->Sender.NodeId());
    auto it = Consumers.find(ev->Sender);
    if (it != Consumers.end()) {
        if (ev->Cookie <= it->second->Generation) {
            LOG_ROW_DISPATCHER_WARN("Consumer already exists, ignore StartSession");
            return;
        }
        LOG_ROW_DISPATCHER_WARN("Consumer already exists, new consumer with new generation (" << ev->Cookie << ", current " 
            << it->second->Generation << "), remove old consumer, sender " << ev->Sender << ", topicPath " 
            << ev->Get()->Record.GetSource().GetTopicPath() << " cookie " << ev->Cookie);
        DeleteConsumer(ev->Sender);
    }
    const auto& source = ev->Get()->Record.GetSource();
    auto consumerInfo = MakeAtomicShared<ConsumerInfo>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record,
        NodesTracker.GetNodeConnected(ev->Sender.NodeId()), ev->Cookie);

    Consumers[ev->Sender] = consumerInfo;
    ConsumersByEventQueueId[consumerInfo->EventQueueId] = consumerInfo;
    if (!CheckSession(consumerInfo, ev)) {
        return;
    }

    for (auto partitionId : ev->Get()->Record.GetPartitionId()) {
        TActorId sessionActorId;
        TopicSessionKey topicKey{source.GetReadGroup(), source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath(), partitionId};
        TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
        Y_ENSURE(topicSessionInfo.Sessions.size() <= 1);

        if (topicSessionInfo.Sessions.empty()) {
            LOG_ROW_DISPATCHER_DEBUG("Create new session: read group " << source.GetReadGroup() << " topic " << source.GetTopicPath() 
                << " part id " << ev->Get()->Record.GetPartitionId() << " offset " << readOffset);
            sessionActorId = ActorFactory->RegisterTopicSession(
                source.GetReadGroup(),
                source.GetTopicPath(),
                source.GetEndpoint(),
                source.GetDatabase(),
                Config,
                SelfId(),
                CompileServiceActorId,
                partitionId,
                YqSharedResources->UserSpaceYdbDriver,
                CreateCredentialsProviderFactoryForStructuredToken(
                    CredentialsFactory,
                    ev->Get()->Record.GetToken(),
                    source.GetAddBearerToToken()),
                Counters,
                PqGateway,
                MaxSessionBufferSizeBytes
                );
            SessionInfo& sessionInfo = topicSessionInfo.Sessions[sessionActorId];
            sessionInfo.Consumers[ev->Sender] = consumerInfo;
        } else {
            auto sessionIt = topicSessionInfo.Sessions.begin();
            SessionInfo& sessionInfo = sessionIt->second;
            sessionInfo.Consumers[ev->Sender] = consumerInfo;
            sessionActorId = sessionIt->first;
        }
        consumerInfo->Partitions[partitionId].TopicSessionId = sessionActorId;

        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStartSession>();
        event->Record.CopyFrom(ev->Get()->Record);
        Send(new IEventHandle(sessionActorId, ev->Sender, event.release(), 0));
    }
    consumerInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvStartSessionAck(consumerInfo->Proto), consumerInfo->Generation);
    Metrics.ClientsCount->Set(Consumers.size());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvGetNextBatch from " << ev->Sender << " part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Received TEvGetNextBatch from " << ev->Sender << " part id " << ev->Get()->Record.GetPartitionId() << " query id " << it->second->QueryId);
    if (!CheckSession(it->second, ev)) {
        return;
    }
    auto& partition = it->second->Partitions[ev->Get()->Record.GetPartitionId()];
    partition.PendingNewDataArrived = false;
    partition.PendingGetNextBatch = true;
    it->second->Counters.GetNextBatch++;
    Forward(ev, partition.TopicSessionId);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Wrong consumer, sender " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Received TEvHeartbeat from " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId() << " query id " << it->second->QueryId);
    CheckSession(it->second, ev);
}

template <class TEventPtr>
bool TRowDispatcher::CheckSession(TAtomicSharedPtr<ConsumerInfo>& consumer, const TEventPtr& ev) {
    if (ev->Cookie != consumer->Generation) {
        LOG_ROW_DISPATCHER_WARN("Wrong message generation (" << typeid(TEventPtr).name()  << "), sender " << ev->Sender << " cookie " << ev->Cookie << ", session generation " << consumer->Generation << ", query id " << consumer->QueryId);
        return false;
    }
    if (!consumer->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        LOG_ROW_DISPATCHER_WARN("Wrong seq num ignore message (" << typeid(TEventPtr).name() << ") seqNo " << meta.GetSeqNo() << " from " << ev->Sender.ToString() << ", query id " << consumer->QueryId);
        return false;
    }
    return true;
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvStopSession from " << ev->Sender);
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Received TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() << " query id " << it->second->QueryId);
    if (!CheckSession(it->second, ev)) {
        return;
    }
    DeleteConsumer(ev->Sender);
}

void TRowDispatcher::DeleteConsumer(NActors::TActorId readActorId) {
    auto consumerIt = Consumers.find(readActorId);
    if (consumerIt == Consumers.end()) {
        LOG_ROW_DISPATCHER_ERROR("Ignore (no consumer) DeleteConsumer, " << " read actor id " << readActorId);
        return;
    }

    const auto& consumer = consumerIt->second;
    LOG_ROW_DISPATCHER_DEBUG("DeleteConsumer, readActorId " << readActorId << " query id " << consumer->QueryId);
    for (auto& [partitionId, partition] : consumer->Partitions) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        *event->Record.MutableSource() = consumer->SourceParams;
        Send(new IEventHandle(partition.TopicSessionId, consumer->ReadActorId, event.release(), 0));

        TopicSessionKey topicKey{
            consumer->SourceParams.GetReadGroup(),
            consumer->SourceParams.GetEndpoint(),
            consumer->SourceParams.GetDatabase(),
            consumer->SourceParams.GetTopicPath(),
            partitionId};
        TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
        SessionInfo& sessionInfo = topicSessionInfo.Sessions[partition.TopicSessionId];
        Y_ENSURE(sessionInfo.Consumers.count(consumer->ReadActorId));
        sessionInfo.Consumers.erase(consumer->ReadActorId);
        if (sessionInfo.Consumers.empty()) {
            LOG_ROW_DISPATCHER_DEBUG("Session is not used, sent TEvPoisonPill to " << partition.TopicSessionId);
            topicSessionInfo.Sessions.erase(partition.TopicSessionId);
            Send(partition.TopicSessionId, new NActors::TEvents::TEvPoisonPill());
            if (topicSessionInfo.Sessions.empty()) {
                TopicSessions.erase(topicKey);
            }
        }
    }
    ConsumersByEventQueueId.erase(consumerIt->second->EventQueueId);
    Consumers.erase(consumerIt);
    Metrics.ClientsCount->Set(Consumers.size());
}

void TRowDispatcher::Handle(const TEvPrivate::TEvTryConnect::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvTryConnect to node id " << ev->Get()->NodeId);
    NodesTracker.TryConnect(ev->Get()->NodeId);
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
    auto it = ConsumersByEventQueueId.find(ev->Get()->EventQueueId);
    if (it == ConsumersByEventQueueId.end()) {
        LOG_ROW_DISPATCHER_TRACE("No consumer with EventQueueId = " << ev->Get()->EventQueueId);
        return;
    }
    auto& sessionInfo = it->second;

    bool needSend = sessionInfo->EventsQueue.Heartbeat();
    if (needSend) {
        LOG_ROW_DISPATCHER_TRACE("Send TEvHeartbeat to " << sessionInfo->ReadActorId << " query id " << sessionInfo->QueryId);
        sessionInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(), sessionInfo->Generation);
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {    
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvNewDataArrived from " << ev->Sender << " part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvNewDataArrived from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << it->second->QueryId);
    auto& partition = it->second->Partitions[ev->Get()->Record.GetPartitionId()];
    partition.PendingNewDataArrived = true;
    it->second->Counters.NewDataArrived++;
    it->second->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvMessageBatch  from " << ev->Sender << " to " << ev->Get()->ReadActorId);
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvMessageBatch from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << it->second->QueryId);
    Metrics.RowsSent->Add(ev->Get()->Record.MessagesSize());
    auto& partition = it->second->Partitions[ev->Get()->Record.GetPartitionId()];
    partition.PendingGetNextBatch = false;
    it->second->Counters.MessageBatch++;
    it->second->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvSessionError from " << ev->Sender << " to " << ev->Get()->ReadActorId);
        return;
    }
    ++*Metrics.ErrorsCount;
    LOG_ROW_DISPATCHER_TRACE("Forward TEvSessionError from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << it->second->QueryId);
    it->second->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
    DeleteConsumer(ev->Get()->ReadActorId);
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvUpdateMetrics::TPtr&) {
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    UpdateMetrics();
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvPrintStateToLog::TPtr&) {
    PrintStateToLog();
    Schedule(TDuration::Seconds(PrintStateToLogPeriodSec), new NFq::TEvPrivate::TEvPrintStateToLog());
}

void TRowDispatcher::PrintStateToLog() {
    auto str = GetInternalState();
    auto buf = TStringBuf(str);
    for (ui64 offset = 0; offset < buf.size(); offset += PrintStateToLogSplitSize) {
        LOG_ROW_DISPATCHER_DEBUG(buf.SubString(offset, PrintStateToLogSplitSize));
    }
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&) {
    LOG_ROW_DISPATCHER_TRACE("TEvPrivate::TEvSendStatistic");

    Schedule(TDuration::Seconds(Config.GetSendStatusPeriodSec()), new NFq::TEvPrivate::TEvSendStatistic());
    for (auto& [actorId, consumer] : Consumers) {
        if (!NodesTracker.GetNodeConnected(actorId.NodeId())) {
            continue;       // Wait Connected to prevent retry_queue increases.
        }
        auto event = std::make_unique<TEvRowDispatcher::TEvStatistics>();
        ui64 readBytes = 0;
        for (auto& [partitionId, partition] : consumer->Partitions) {
            if (!partition.StatisticsUpdated) {
                continue;
            }
            auto* partitionsProto = event->Record.AddPartition();
            partitionsProto->SetPartitionId(partitionId);
            partitionsProto->SetNextMessageOffset(partition.Stat.Offset);
            readBytes += partition.Stat.ReadBytes;
            partition.Stat.Clear();
            partition.StatisticsUpdated = false;
        }
        event->Record.SetReadBytes(readBytes);
        consumer->EventsQueue.Send(event.release(), consumer->Generation);
    }
}

void TRowDispatcher::Handle(const NMon::TEvHttpInfo::TPtr& ev) {
    UpdateReadActorsInternalState();
    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Current Time: " << TInstant::Now() << Endl;
            str << "Current state:" << Endl;
            str << GetInternalState() << Endl;
            str << "Read actors state: " << Endl;
            str << GetReadActorsInternalState() << Endl;
            str << Endl;
        }
    }
    Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionStatistic::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvSessionStatistic from " << ev->Sender);
    const auto& key = ev->Get()->Stat.SessionKey;
    TopicSessionKey sessionKey{key.ReadGroup, key.Endpoint, key.Database, key.TopicPath, key.PartitionId};

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
    sessionInfo.Stat.Add(ev->Get()->Stat.Common);
    for (const auto& clientStat : ev->Get()->Stat.Clients) {
        auto it = sessionInfo.Consumers.find(clientStat.ReadActorId);
        if (it == sessionInfo.Consumers.end()) {
            continue;
        }
        auto consumerInfoPtr = it->second; 
        consumerInfoPtr->Stat.Add(clientStat);
        auto partitionIt = consumerInfoPtr->Partitions.find(key.PartitionId);
        if ( partitionIt == consumerInfoPtr->Partitions.end())
        {
            continue;
        }
        partitionIt->second.Stat.Add(clientStat);
        partitionIt->second.StatisticsUpdated = true;
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetInternalStateResponse::TPtr& ev) {
    auto& readActorInternalState = ReadActorsInternalState[ev->Sender];
    readActorInternalState.InternalState = ev->Get()->Record.GetInternalState();
    readActorInternalState.ResponseTime = TInstant::Now();
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
    const NYql::IPqGateway::TPtr& pqGateway,
    NActors::TMon* monitoring)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
        config,
        credentialsProviderFactory,
        yqSharedResources,
        credentialsFactory,
        tenant,
        actorFactory,
        counters,
        pqGateway,
        monitoring));
}

} // namespace NFq
