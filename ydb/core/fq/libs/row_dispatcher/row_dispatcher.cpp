#include "row_dispatcher.h"

#include "actors_factory.h"
#include "coordinator.h"
#include "leader_election.h"
#include "local_leader_election.h"
#include "probes.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/metrics/sanitize_label.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

#include <yql/essentials/public/purecalc/common/interface.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/queue.h>
#include <util/stream/format.h>

namespace NFq {

using namespace NActors;

LWTRACE_USING(FQ_ROW_DISPATCHER_PROVIDER);

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

struct TUserPoolMetrics {
    explicit TUserPoolMetrics(const ::NMonitoring::TDynamicCounterPtr& utilsCounters) {
        auto execpoolGroup = utilsCounters->GetSubgroup("execpool", "User");
        auto microsecGroup = execpoolGroup->GetSubgroup("sensor", "ElapsedMicrosecByActivity");
        Session = microsecGroup->GetNamedCounter("activity", "FQ_ROW_DISPATCHER_SESSION", true);
        RowDispatcher = microsecGroup->GetNamedCounter("activity", "FQ_ROW_DISPATCHER", true);
        CompilerActor = microsecGroup->GetNamedCounter("activity", "FQ_ROW_DISPATCHER_COMPILE_ACTOR", true);
        CompilerService = microsecGroup->GetNamedCounter("activity", "FQ_ROW_DISPATCHER_COMPILE_SERVICE", true);
        FormatHandler = microsecGroup->GetNamedCounter("activity", "FQ_ROW_DISPATCHER_FORMAT_HANDLER", true);
    }
    ::NMonitoring::TDynamicCounters::TCounterPtr Session;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowDispatcher;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompilerActor;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompilerService;
    ::NMonitoring::TDynamicCounters::TCounterPtr FormatHandler;
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
    TAggQueryStat() = default;
    TAggQueryStat(const TString& queryId, const ::NMonitoring::TDynamicCounterPtr& counters, const NYql::NPq::NProto::TDqPqTopicSource& sourceParams, bool enableStreamingQueriesCounters)
        : QueryId(queryId)
        , SubGroup(counters) {
        auto topicGroup = SubGroup;
        if (enableStreamingQueriesCounters) {
            for (const auto& sensor : sourceParams.GetTaskSensorLabel()) {
                SubGroup = SubGroup->GetSubgroup(sensor.GetLabel(), sensor.GetValue());
            }
            SubGroup = SubGroup->GetSubgroup("query_id", queryId);
            topicGroup = SubGroup->GetSubgroup("read_group", SanitizeLabel(sourceParams.GetReadGroup()));
        }
        MaxQueuedBytesCounter = topicGroup->GetCounter("MaxQueuedBytes");
        AvgQueuedBytesCounter = topicGroup->GetCounter("AvgQueuedBytes");
        MaxReadLagCounter = topicGroup->GetCounter("MaxReadLag");
    }

    TString QueryId;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr MaxQueuedBytesCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr AvgQueuedBytesCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr MaxReadLagCounter;

    NYql::TCounters::TEntry FilteredBytes;
    NYql::TCounters::TEntry QueuedBytes;
    NYql::TCounters::TEntry QueuedRows;
    NYql::TCounters::TEntry ReadLagMessages;
    bool IsWaiting = false;
    bool Updated = false;

    void Add(const TTopicSessionClientStatistic& stat, ui64 filteredBytes) {
        FilteredBytes.Add(NYql::TCounters::TEntry(filteredBytes));
        QueuedBytes.Add(NYql::TCounters::TEntry(stat.QueuedBytes));
        QueuedRows.Add(NYql::TCounters::TEntry(stat.QueuedRows));
        ReadLagMessages.Add(NYql::TCounters::TEntry(stat.ReadLagMessages));
        IsWaiting = IsWaiting || stat.IsWaiting;
        Updated = true;
    }

    void SetMetrics() {
        SetMetrics(QueuedBytes.Max, QueuedBytes.Avg, ReadLagMessages.Max);
    }

    void SetMetrics(ui64 queuedBytesMax, ui64 queuedBytesAvg, i64 readLagMessagesMax) {
        if (!SubGroup) {
            return;
        }
        MaxQueuedBytesCounter->Set(queuedBytesMax);
        AvgQueuedBytesCounter->Set(queuedBytesAvg);
        MaxReadLagCounter->Set(readLagMessagesMax);
    }

    void Remove() {
        if (!SubGroup) {
            return;
        }
        SetMetrics(0, 0, 0);
        SubGroup->RemoveSubgroup("query_id", QueryId);
    }

    void Clear() {
        Updated = false;
        FilteredBytes = NYql::TCounters::TEntry{};
        QueuedBytes = NYql::TCounters::TEntry{};
        QueuedRows = NYql::TCounters::TEntry{};
        ReadLagMessages = NYql::TCounters::TEntry{};
        IsWaiting = false;
    }
};

ui64 UpdateMetricsPeriodSec = 60;
ui64 PrintStateToLogPeriodSec = 600;
ui64 PrintStateToLogSplitSize = 64000;
ui64 MaxSessionBufferSizeBytes = 16000000;

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {

    struct TTopicSessionKey {
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
        bool operator==(const TTopicSessionKey& other) const {
            return ReadGroup == other.ReadGroup && Endpoint == other.Endpoint && Database == other.Database
                && TopicPath == other.TopicPath && PartitionId == other.PartitionId;
        }
    };

    struct TTopicSessionKeyHash {
        int operator()(const TTopicSessionKey& k) const {
            return k.Hash();
        }
    };

     struct TNodesTracker {
        explicit TNodesTracker(TDuration timeout)
            : Timeout(timeout)
        {}

        class TRetryState {
        public:
            explicit TRetryState(TDuration timeout)
                : Timeout(timeout)
            {}
            TDuration GetNextDelay() {
                constexpr TDuration MaxDelay = TDuration::Seconds(10);
                constexpr TDuration MinDelay = TDuration::MilliSeconds(100); // from second retry
                TDuration ret = Delay; // The first delay is zero
                Delay = ClampVal(Delay * 2, MinDelay, MaxDelay);
                return ret ? RandomizeDelay(ret) : ret;
            }

            bool IsTimeout() const {
                return TInstant::Now() - DisconnectedTime > Timeout;
            }
        private:
            static TDuration RandomizeDelay(TDuration baseDelay) {
                const TDuration::TValue half = baseDelay.GetValue() / 2;
                return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
            }
        private:
            TDuration Delay; // The first time retry will be done instantly.
            TInstant DisconnectedTime = TInstant::Now();
            TDuration Timeout;
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

        bool HandleNodeDisconnected(ui32 nodeId) {
            auto& state = Nodes[nodeId];
            state.Connected = false;
            state.Counters.Disconnected++;
            if (state.RetryScheduled) {
                return false;
            }
            state.RetryScheduled = true;
            if (!state.RetryState) {
                state.RetryState.ConstructInPlace(Timeout);
            }

            if (state.RetryState->IsTimeout()) {
                return true;
            }

            auto ev = MakeHolder<TEvPrivate::TEvTryConnect>(nodeId);
            auto delay = state.RetryState->GetNextDelay();
            NActors::TActivationContext::Schedule(delay, new NActors::IEventHandle(SelfId, SelfId, ev.Release()));
            return false;
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
        TDuration Timeout;
    };

    struct TAggregatedStats{
        THashMap<TQueryStatKey, TAggQueryStat, TQueryStatKeyHash> LastQueryStats;
        TDuration LastUpdateMetricsPeriod;
    };

    const TRowDispatcherSettings Config;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TActorId CompileServiceActorId;
    TMaybe<TActorId> CoordinatorActorId;
    ui64 CoordinatorGeneration = 0;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    const TString LogPrefix;
    ui64 NextEventQueueId = 0;
    TString Tenant;
    NFq::NRowDispatcher::IActorFactory::TPtr ActorFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    const ::NMonitoring::TDynamicCounterPtr CountersRoot;
    TRowDispatcherMetrics Metrics;
    TUserPoolMetrics UserPoolMetrics;
    NYql::IPqGateway::TPtr PqGateway;
    NYdb::TDriver Driver;
    NActors::TMon* Monitoring;
    TNodesTracker NodesTracker;
    NYql::TCounters::TEntry AllSessionsDateRate;
    TAggregatedStats AggrStats; 
    ui64 LastCpuTime = 0;
    NActors::TActorId NodesManagerId;

    struct TConsumerCounters {
        ui64 NewDataArrived = 0;
        ui64 GetNextBatch = 0;
        ui64 MessageBatch = 0;
    };

    struct TConsumerPartition {
        bool PendingGetNextBatch = false;
        bool PendingNewDataArrived = false;
        TActorId TopicSessionId;
        TTopicSessionClientStatistic Stat;
        ui64 FilteredBytes = 0;
        bool StatisticsUpdated = false;
    };

    struct TConsumerInfo {
        TConsumerInfo(
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
        TConsumerCounters Counters;
        ui64 CpuMicrosec = 0;               // Increment.
        ui64 Generation;
    };

    struct TSessionInfo {
        TMap<TActorId, TAtomicSharedPtr<TConsumerInfo>> Consumers;   // key - ReadActor actor id
        TTopicSessionCommonStatistic Stat;                           // Increments
        NYql::TCounters::TEntry AggrReadBytes;
    };

    struct TTopicSessionInfo {
        TMap<TActorId, TSessionInfo> Sessions;                         // key - TopicSession actor id
    };

    struct TReadActorInfo {
        TString InternalState;
        TInstant RequestTime;
        TInstant ResponseTime;
    };

    THashMap<NActors::TActorId, TAtomicSharedPtr<TConsumerInfo>> Consumers;      // key - read actor id
    TMap<ui64, TAtomicSharedPtr<TConsumerInfo>> ConsumersByEventQueueId;
    THashMap<TTopicSessionKey, TTopicSessionInfo, TTopicSessionKeyHash> TopicSessions;
    TMap<TActorId, TReadActorInfo> ReadActorsInternalState;
    bool EnableStreamingQueriesCounters = false;

public:
    explicit TRowDispatcher(
        const TRowDispatcherSettings& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& tenant,
        const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NMonitoring::TDynamicCounterPtr& countersRoot,
        const NYql::IPqGateway::TPtr& pqGateway,
        NYdb::TDriver driver,
        NActors::TMon* monitoring = nullptr,
        NActors::TActorId nodesManagerId = {},
        bool enableStreamingQueriesCounters = false
    );

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
    void Handle(NFq::TEvRowDispatcher::TEvNoSession::TPtr& ev);

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
    bool CheckSession(TAtomicSharedPtr<TConsumerInfo>& consumer, const TEventPtr& ev);
    void PrintStateToLog();
    void UpdateCpuTime();

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
        hFunc(NFq::TEvRowDispatcher::TEvNoSession, Handle);
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
    const TRowDispatcherSettings& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    NYdb::TDriver driver,
    NActors::TMon* monitoring,
    NActors::TActorId nodesManagerId,
    bool enableStreamingQueriesCounters)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , CredentialsFactory(credentialsFactory)
    , FunctionRegistry(functionRegistry)
    , LogPrefix("RowDispatcher: ")
    , Tenant(tenant)
    , ActorFactory(actorFactory)
    , Counters(counters)
    , CountersRoot(countersRoot)
    , Metrics(counters)
    , UserPoolMetrics(countersRoot->GetSubgroup("counters", "utils"))
    , PqGateway(pqGateway)
    , Driver(driver)
    , Monitoring(monitoring)
    , NodesTracker(Config.GetCoordinator().GetRebalancingTimeout() ? Config.GetCoordinator().GetRebalancingTimeout() : TDuration::Seconds(DefaultRebalancingTimeoutSec))
    , NodesManagerId(nodesManagerId)
    , EnableStreamingQueriesCounters(enableStreamingQueriesCounters)
{
    Y_ENSURE(!Tenant.empty());
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId() << ", tenant " << Tenant);

    const auto& config = Config.GetCoordinator();
    auto coordinatorId = Register(NewCoordinator(SelfId(), config, Tenant, Counters, NodesManagerId).release());
    auto leaderElection = !config.GetCoordinationNodePath().empty()
        ? NewLeaderElection(SelfId(), coordinatorId, config, CredentialsProviderFactory, Driver, Tenant, Counters)
        : NewLocalLeaderElection(SelfId(), coordinatorId, Counters);
    Register(leaderElection.release());

    CompileServiceActorId = Register(NRowDispatcher::CreatePurecalcCompileService(Config.GetCompileService(), Counters));

    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    // Schedule(TDuration::Seconds(PrintStateToLogPeriodSec), new NFq::TEvPrivate::TEvPrintStateToLog());  // Logs (InternalState) is too big
    Y_ENSURE(Config.GetSendStatusPeriod() > TDuration::Zero());
    Schedule(Config.GetSendStatusPeriod(), new NFq::TEvPrivate::TEvSendStatistic());

    if (Monitoring) {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(FQ_ROW_DISPATCHER_PROVIDER));
        ::NMonitoring::TIndexMonPage* actorsMonPage = Monitoring->RegisterIndexPage("actors", "Actors");
        Monitoring->RegisterActorPage(actorsMonPage, "row_dispatcher", "Row Dispatcher", false,
            TlsActivationContext->ActorSystem(), SelfId());
    }
    NodesTracker.Init(SelfId());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LWPROBE(CoordinatorChanged, ev->Sender.ToString(), ev->Get()->Generation, ev->Get()->CoordinatorActorId.ToString(), CoordinatorGeneration, CoordinatorActorId->ToString());
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
    LWPROBE(NodeConnected, ev->Sender.ToString(), ev->Get()->NodeId);
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected, node id " << ev->Get()->NodeId);
    Metrics.NodesReconnect->Inc();
    NodesTracker.HandleNodeConnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LWPROBE(NodeDisconnected, ev->Sender.ToString(), ev->Get()->NodeId);
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    Metrics.NodesReconnect->Inc();
    bool isTimeout = NodesTracker.HandleNodeDisconnected(ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    TVector<TActorId> toDelete;
    if (isTimeout) {
        LOG_ROW_DISPATCHER_DEBUG("Node disconnected, node id " << ev->Get()->NodeId << " is timeout");
        for (auto& [actorId, consumer] : Consumers) {
            if (actorId.NodeId() != ev->Get()->NodeId) {
                continue;
            }
            toDelete.push_back(actorId);
        }   
    }
    for (auto& actorId : toDelete) {
        DeleteConsumer(actorId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LWPROBE(UndeliveredStart, ev->Sender.ToString(), ev->Get()->Reason, ev->Cookie);
    LOG_ROW_DISPATCHER_TRACE("TEvUndelivered, from " << ev->Sender << ", reason " << ev->Get()->Reason);
    for (auto& [key, consumer] : Consumers) {
        if (ev->Cookie != consumer->Generation) {       // Several partitions in one read_actor have different Generation.
            LWPROBE(UndeliveredSkipGeneration, ev->Sender.ToString(), ev->Get()->Reason, ev->Cookie, consumer->Generation);
            continue;
        }
        if (consumer->EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            LWPROBE(UndeliveredDeleteConsumer, ev->Sender.ToString(), ev->Get()->Reason, ev->Cookie, key.ToString());
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
    LWPROBE(CoordinatorPing, CoordinatorActorId->ToString());
    LOG_ROW_DISPATCHER_TRACE("Send ping to " << *CoordinatorActorId);
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing());
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPong::TPtr&) {
    LWPROBE(Pong);
    LOG_ROW_DISPATCHER_TRACE("NActors::TEvents::TEvPong");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChangesSubscribe from " << ev->Sender);
    NodesTracker.AddNode(ev->Sender.NodeId());
    CoordinatorChangedSubscribers.insert(ev->Sender);
    if (!CoordinatorActorId) {
        return;
    }
    LWPROBE(CoordinatorChangesSubscribe, ev->Sender.ToString(), CoordinatorGeneration, CoordinatorActorId->ToString());
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

    AllSessionsDateRate = NYql::TCounters::TEntry();
    for (auto& [queryId, stat] : AggrStats.LastQueryStats) {
        stat.Clear();
    }

    for (auto& [key, sessionsInfo] : TopicSessions) {
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            auto read = NYql::TCounters::TEntry(sessionInfo.Stat.ReadBytes);
            AllSessionsDateRate.Add(read);
            sessionInfo.AggrReadBytes = read;
            sessionInfo.Stat.Clear();

            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                const auto partionIt = consumer->Partitions.find(key.PartitionId);
                if (partionIt == consumer->Partitions.end()) {
                    continue;
                }
                auto& partition = partionIt->second;
                TQueryStatKey statKey{consumer->QueryId, key.ReadGroup};
                auto& stats = AggrStats.LastQueryStats.emplace(
                    statKey,
                    TAggQueryStat(consumer->QueryId, Metrics.Counters, consumer->SourceParams, EnableStreamingQueriesCounters)).first->second;
                stats.Add(partition.Stat, partition.FilteredBytes);
                partition.FilteredBytes = 0;
            }
        }
    }
    THashSet<TQueryStatKey, TQueryStatKeyHash> toDelete;
    for (auto& [key, stats] : AggrStats.LastQueryStats) {
        if (!stats.Updated) {
            toDelete.insert(key);
            stats.Remove();
            continue;
        }
        stats.SetMetrics();
    }
    for (const auto& key : toDelete) {
         AggrStats.LastQueryStats.erase(key);
    }
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
    str << "CpuMicrosec: " << toHuman(LastCpuTime) << "\n";
    str << "DataRate (all sessions): ";
    printDataRate(AllSessionsDateRate);
    str << "\n";

    THashMap<TQueryStatKey, TAggQueryStat, TQueryStatKeyHash> queryState;
    THashMap<TQueryStatKey, ui64, TQueryStatKeyHash> sessionCountByQuery;
    ui64 queuedBytesSum = 0;

    for (auto& [sessionKey, sessionsInfo] : TopicSessions) {
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            queuedBytesSum += sessionInfo.Stat.QueuedBytes;
            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                const auto partionIt = consumer->Partitions.find(sessionKey.PartitionId);
                if (partionIt == consumer->Partitions.end()) {
                    continue;
                }
                const auto& partitionStat = partionIt->second.Stat;
                auto key = TQueryStatKey{consumer->QueryId, sessionKey.ReadGroup};
                ++sessionCountByQuery[key];
                queryState[key].Add(partitionStat, 0);
            }
        }
    }

    if (TopicSessions.size()) {
        str << "Buffer used: " <<  Prec(queuedBytesSum * 100.0 / (TopicSessions.size() * MaxSessionBufferSizeBytes), 4) << "% (" << toHuman(queuedBytesSum) << ")\n";
    }

    str << "Queries:\n";
    for (const auto& [queryStatKey, stat]: queryState) {
        auto [queryId, readGroup] = queryStatKey;
        auto sessionsBufferSumSize = sessionCountByQuery[queryStatKey] * MaxSessionBufferSizeBytes;
        auto used = sessionsBufferSumSize ? (stat.QueuedBytes.Sum * 100.0 / sessionsBufferSumSize) : 0.0;
        str << "  " << queryId << " / " << readGroup << ": buffer used (all partitions) " << LeftPad(Prec(used, 4), 10) << "% (" << toHuman(stat.QueuedBytes.Sum) <<  ") unread max (one partition) " << toHuman(stat.QueuedBytes.Max) << " data rate";
        auto statIt = AggrStats.LastQueryStats.find(queryStatKey);
        if (statIt != AggrStats.LastQueryStats.end()) {
            printDataRate(statIt->second.FilteredBytes);
        }
        str << " waiting " << stat.IsWaiting << " max read lag " << stat.ReadLagMessages.Max;
        str << "\n";
    }
    str << "TopicSessions:\n";
    for (auto& [key, sessionsInfo] : TopicSessions) {
        str << "  " << key.TopicPath << " / " << key.PartitionId  << " / " << key.ReadGroup;
        for (auto& [actorId, sessionInfo] : sessionsInfo.Sessions) {
            str << " / " << LeftPad(actorId, 32)
                << " data rate " << toHumanDR(sessionInfo.AggrReadBytes.Sum) << " unread bytes " << toHuman(sessionInfo.Stat.QueuedBytes)
                << " offset " << LeftPad(sessionInfo.Stat.LastReadedOffset, 12) << " restarts by offsets " << sessionInfo.Stat.RestartSessionByOffsets << "\n";
            ui64 maxInitialOffset = 0;
            ui64 minInitialOffset = std::numeric_limits<ui64>::max();

            for (const auto& [formatName, formatStats] : sessionInfo.Stat.FormatHandlers) {
                str << "    " << formatName 
                    << " parse and filter lantecy  " << formatStats.ParseAndFilterLatency
                    << " (parse " << formatStats.ParserStats.ParserLatency << ", filter " << formatStats.FilterStats.FilterLatency << ")\n";
            }

            for (auto& [readActorId, consumer] : sessionInfo.Consumers) {
                if (!consumer->Partitions.contains(key.PartitionId)) {
                    continue;
                }
                const auto& partition = consumer->Partitions[key.PartitionId];
                const auto& stat = partition.Stat;
                str << "    " << consumer->QueryId << " " << LeftPad(readActorId, 33) << " unread bytes "
                    << toHuman(stat.QueuedBytes) << " (" << leftPad(stat.QueuedRows) << " rows) "
                    << " offset " << leftPad(stat.Offset) << " init offset " << leftPad(stat.InitialOffset)
                    << " get " << leftPad(consumer->Counters.GetNextBatch)
                    << " arr " << leftPad(consumer->Counters.NewDataArrived) << " btc " << leftPad(consumer->Counters.MessageBatch) 
                    << " pend get " << leftPad(partition.PendingGetNextBatch) << " pend new " << leftPad(partition.PendingNewDataArrived)
                    << " waiting " <<  stat.IsWaiting << " read lag " << leftPad(stat.ReadLagMessages) 
                    << " conn id " <<  consumer->Generation << "\n";
                maxInitialOffset = std::max(maxInitialOffset, stat.InitialOffset);
                minInitialOffset = std::min(minInitialOffset, stat.InitialOffset);
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
        " part id " << JoinSeq(',', ev->Get()->Record.GetPartitionIds()) << " query id " << ev->Get()->Record.GetQueryId() << " cookie " << ev->Cookie);
    if (EnableStreamingQueriesCounters) {
        auto queryGroup = Metrics.Counters->GetSubgroup("query_id", ev->Get()->Record.GetQueryId());
        auto topicGroup = queryGroup->GetSubgroup("read_group", SanitizeLabel(ev->Get()->Record.GetSource().GetReadGroup()));
        topicGroup->GetCounter("StartSession", true)->Inc();
    }

    LWPROBE(StartSession, ev->Sender.ToString(), ev->Get()->Record.GetQueryId(), ev->Get()->Record.ByteSizeLong());

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
    auto consumerInfo = MakeAtomicShared<TConsumerInfo>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record,
        NodesTracker.GetNodeConnected(ev->Sender.NodeId()), ev->Cookie);

    Consumers[ev->Sender] = consumerInfo;
    ConsumersByEventQueueId[consumerInfo->EventQueueId] = consumerInfo;
    if (!CheckSession(consumerInfo, ev)) {
        return;
    }

    for (auto partitionId : ev->Get()->Record.GetPartitionIds()) {
        TActorId sessionActorId;
        TTopicSessionKey topicKey{source.GetReadGroup(), source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath(), partitionId};
        TTopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
        Y_ENSURE(topicSessionInfo.Sessions.size() <= 1);

        if (topicSessionInfo.Sessions.empty()) {
            LOG_ROW_DISPATCHER_DEBUG("Create new session: read group " << source.GetReadGroup() << " topic " << source.GetTopicPath() 
                << " part id " << partitionId);
            sessionActorId = ActorFactory->RegisterTopicSession(
                source.GetReadGroup(),
                source.GetTopicPath(),
                source.GetEndpoint(),
                source.GetDatabase(),
                Config,
                FunctionRegistry,
                SelfId(),
                CompileServiceActorId,
                partitionId,
                Driver,
                CreateCredentialsProviderFactoryForStructuredToken(
                    CredentialsFactory,
                    ev->Get()->Record.GetToken(),
                    source.GetAddBearerToToken()),
                Counters,
                CountersRoot,
                PqGateway,
                MaxSessionBufferSizeBytes,
                EnableStreamingQueriesCounters
                );
            TSessionInfo& sessionInfo = topicSessionInfo.Sessions[sessionActorId];
            sessionInfo.Consumers[ev->Sender] = consumerInfo;
        } else {
            auto sessionIt = topicSessionInfo.Sessions.begin();
            TSessionInfo& sessionInfo = sessionIt->second;
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
    auto& session = it->second;
    LWPROBE(GetNextBatch, ev->Sender.ToString(), ev->Get()->Record.GetPartitionId(), session->QueryId, ev->Get()->Record.ByteSizeLong());
    LOG_ROW_DISPATCHER_TRACE("Received TEvGetNextBatch from " << ev->Sender << " part id " << ev->Get()->Record.GetPartitionId() << " query id " << it->second->QueryId);
    if (!CheckSession(session, ev)) {
        return;
    }
    auto partitionIt = session->Partitions.find(ev->Get()->Record.GetPartitionId());
    if (partitionIt == session->Partitions.end()) {
        LOG_ROW_DISPATCHER_ERROR("Ignore TEvGetNextBatch from " << ev->Sender << ", wrong partition id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    partitionIt->second.PendingNewDataArrived = false;
    partitionIt->second.PendingGetNextBatch = true;
    session->Counters.GetNextBatch++;
    Forward(ev, partitionIt->second.TopicSessionId);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Wrong consumer, sender " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    LWPROBE(Heartbeat, ev->Sender.ToString(), ev->Get()->Record.GetPartitionId(), it->second->QueryId, ev->Get()->Record.ByteSizeLong());
    LOG_ROW_DISPATCHER_TRACE("Received TEvHeartbeat from " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId() << " query id " << it->second->QueryId);
    CheckSession(it->second, ev);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNoSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Received TEvNoSession from " << ev->Sender << ", generation " << ev->Cookie);
    auto consumerIt = Consumers.find(ev->Sender);
    if (consumerIt == Consumers.end()) {
        return;
    }
    const auto& consumer = consumerIt->second;
    if (consumer->Generation != ev->Cookie) {
        return;
    }
    DeleteConsumer(ev->Sender);
}

template <class TEventPtr>
bool TRowDispatcher::CheckSession(TAtomicSharedPtr<TConsumerInfo>& consumer, const TEventPtr& ev) {
    if (ev->Cookie != consumer->Generation) {
        LOG_ROW_DISPATCHER_WARN("Wrong message generation (" << typeid(TEventPtr).name()  << "), sender " << ev->Sender << " cookie " << ev->Cookie << ", session generation " << consumer->Generation << ", query id " << consumer->QueryId);
        return false;
    }
    if (!consumer->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        LOG_ROW_DISPATCHER_WARN("Wrong seq num, ignore message (" << typeid(TEventPtr).name() << ") seqNo " << meta.GetSeqNo() << " from " << ev->Sender.ToString() << ", query id " << consumer->QueryId);
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

    LWPROBE(StopSession, ev->Sender.ToString(), it->second->QueryId, ev->Get()->Record.ByteSizeLong());
    LOG_ROW_DISPATCHER_DEBUG("Received TEvStopSession from " << ev->Sender << " topic " << ev->Get()->Record.GetSource().GetTopicPath() << " query id " << it->second->QueryId);
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
    LOG_ROW_DISPATCHER_DEBUG("DeleteConsumer, readActorId " << readActorId << " query id " << consumer->QueryId << ", partitions size " << consumer->Partitions.size());
    for (auto& [partitionId, partition] : consumer->Partitions) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        *event->Record.MutableSource() = consumer->SourceParams;
        Send(new IEventHandle(partition.TopicSessionId, consumer->ReadActorId, event.release(), 0));

        TTopicSessionKey topicKey{
            consumer->SourceParams.GetReadGroup(),
            consumer->SourceParams.GetEndpoint(),
            consumer->SourceParams.GetDatabase(),
            consumer->SourceParams.GetTopicPath(),
            partitionId};
        auto sessionIt = TopicSessions.find(topicKey);
        if (sessionIt != TopicSessions.end()) {
            TTopicSessionInfo& topicSessionInfo = sessionIt->second;
            TSessionInfo& sessionInfo = topicSessionInfo.Sessions[partition.TopicSessionId];
            if (!sessionInfo.Consumers.erase(consumer->ReadActorId)) {
                LOG_ROW_DISPATCHER_ERROR("Wrong readActorId " << consumer->ReadActorId << ", no such consumer");
            }
            if (sessionInfo.Consumers.empty()) {
                LOG_ROW_DISPATCHER_DEBUG("Session is not used, sent TEvPoisonPill to " << partition.TopicSessionId);
                topicSessionInfo.Sessions.erase(partition.TopicSessionId);
                Send(partition.TopicSessionId, new NActors::TEvents::TEvPoisonPill());
                if (topicSessionInfo.Sessions.empty()) {
                    TopicSessions.erase(sessionIt);
                }
            }
        }
    }
    ConsumersByEventQueueId.erase(consumerIt->second->EventQueueId);
    Consumers.erase(consumerIt);
    Metrics.ClientsCount->Set(Consumers.size());
}

void TRowDispatcher::Handle(const TEvPrivate::TEvTryConnect::TPtr& ev) {
    LWPROBE(TryConnect, ev->Sender.ToString(), ev->Get()->NodeId);
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
    LWPROBE(PrivateHeartbeat, ev->Sender.ToString(), sessionInfo->QueryId, sessionInfo->Generation);

    bool needSend = sessionInfo->EventsQueue.Heartbeat();
    if (needSend) {
        LOG_ROW_DISPATCHER_TRACE("Send TEvHeartbeat to " << sessionInfo->ReadActorId << " query id " << sessionInfo->QueryId);
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvHeartbeat>();
        sessionInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(), sessionInfo->Generation);
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {    
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvNewDataArrived from " << ev->Sender << " part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    auto consumerInfoPtr = it->second; 
    LWPROBE(NewDataArrived, ev->Sender.ToString(), ev->Get()->ReadActorId.ToString(), consumerInfoPtr->QueryId, consumerInfoPtr->Generation, ev->Get()->Record.ByteSizeLong());
    LOG_ROW_DISPATCHER_TRACE("Forward TEvNewDataArrived from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << consumerInfoPtr->QueryId);
    auto partitionIt = consumerInfoPtr->Partitions.find(ev->Get()->Record.GetPartitionId());
    if (partitionIt == consumerInfoPtr->Partitions.end()) {
        // Ignore TEvNewDataArrived because read actor now read others partitions.
        return;
    }
    partitionIt->second.PendingNewDataArrived = true;
    consumerInfoPtr->Counters.NewDataArrived++;
    consumerInfoPtr->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvMessageBatch  from " << ev->Sender << " to " << ev->Get()->ReadActorId);
        return;
    }
    auto consumerInfoPtr = it->second; 
    LWPROBE(MessageBatch, ev->Sender.ToString(), ev->Get()->ReadActorId.ToString(), consumerInfoPtr->QueryId, consumerInfoPtr->Generation, ev->Get()->Record.ByteSizeLong());
    LOG_ROW_DISPATCHER_TRACE("Forward TEvMessageBatch from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << consumerInfoPtr->QueryId);
    Metrics.RowsSent->Add(ev->Get()->Record.MessagesSize());
    auto partitionIt = consumerInfoPtr->Partitions.find(ev->Get()->Record.GetPartitionId());
    if (partitionIt == consumerInfoPtr->Partitions.end()) {
        // Ignore TEvMessageBatch because read actor now read others partitions.
        return;
    }
    partitionIt->second.PendingGetNextBatch = false;
    consumerInfoPtr->Counters.MessageBatch++;
    consumerInfoPtr->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    auto it = Consumers.find(ev->Get()->ReadActorId);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore (no consumer) TEvSessionError from " << ev->Sender << " to " << ev->Get()->ReadActorId);
        return;
    }
    LWPROBE(SessionError, ev->Sender.ToString(), ev->Get()->ReadActorId.ToString(), it->second->QueryId, it->second->Generation, ev->Get()->Record.ByteSizeLong());
    ++*Metrics.ErrorsCount;
    LOG_ROW_DISPATCHER_TRACE("Forward TEvSessionError from " << ev->Sender << " to " << ev->Get()->ReadActorId << " query id " << it->second->QueryId);

    if (ev->Get()->IsFatalError) {
        auto consumerIt = Consumers.find(ev->Get()->ReadActorId);
        if (consumerIt == Consumers.end()) {
            LOG_ROW_DISPATCHER_ERROR("Ignore (no consumer) DeleteConsumer, " << " read actor id " << ev->Get()->ReadActorId);
            return;
        }
        const auto& consumer = consumerIt->second;
        TTopicSessionKey topicKey{
            consumer->SourceParams.GetReadGroup(),
            consumer->SourceParams.GetEndpoint(),
            consumer->SourceParams.GetDatabase(),
            consumer->SourceParams.GetTopicPath(),
            ev->Get()->Record.GetPartitionId()};

        auto sessionIt = TopicSessions.find(topicKey);
        if (sessionIt != TopicSessions.end()) {
            TTopicSessionInfo& topicSessionInfo = sessionIt->second;
            if (topicSessionInfo.Sessions.erase(ev->Sender)) {
                LOG_ROW_DISPATCHER_WARN("Fatal session error, remove session " << ev->Sender);
                Send(ev->Sender, new NActors::TEvents::TEvPoisonPill());
                if (topicSessionInfo.Sessions.empty()) {
                    TopicSessions.erase(sessionIt);
                }
            }
        }
    }
    auto readActorId = ev->Get()->ReadActorId;
    it->second->EventsQueue.Send(ev->Release().Release(), it->second->Generation);
    DeleteConsumer(readActorId);
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvUpdateMetrics::TPtr&) {
    LWPROBE(UpdateMetrics);
    Schedule(TDuration::Seconds(UpdateMetricsPeriodSec), new NFq::TEvPrivate::TEvUpdateMetrics());
    UpdateMetrics();
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvPrintStateToLog::TPtr&) {
    LWPROBE(PrintStateToLog, PrintStateToLogPeriodSec);
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

    UpdateCpuTime();
    Schedule(Config.GetSendStatusPeriod(), new NFq::TEvPrivate::TEvSendStatistic());
    for (auto& [actorId, consumer] : Consumers) {
        if (!NodesTracker.GetNodeConnected(actorId.NodeId())) {
            continue;       // Wait Connected to prevent retry_queue increases.
        }
        auto event = std::make_unique<TEvRowDispatcher::TEvStatistics>();
        ui64 readBytes = 0;
        ui64 filteredBytes = 0;
        ui64 filteredRows = 0;
        ui64 queuedBytes = 0;
        ui64 queuedRows = 0;
        for (auto& [partitionId, partition] : consumer->Partitions) {
            if (!partition.StatisticsUpdated) {
                continue;
            }
            auto* partitionsProto = event->Record.AddPartition();
            partitionsProto->SetPartitionId(partitionId);
            if (partition.Stat.Offset) {
                partitionsProto->SetNextMessageOffset(*partition.Stat.Offset);
            }
            readBytes += partition.Stat.ReadBytes;
            filteredBytes += partition.Stat.FilteredBytes;
            filteredRows += partition.Stat.FilteredRows;
            queuedBytes += partition.Stat.QueuedBytes;
            queuedRows += partition.Stat.QueuedRows;
            partition.Stat.Clear();
            partition.StatisticsUpdated = false;
        }
        event->Record.SetReadBytes(readBytes);
        event->Record.SetCpuMicrosec(consumer->CpuMicrosec);
        consumer->CpuMicrosec = 0;
        event->Record.SetFilteredBytes(filteredBytes);
        event->Record.SetFilteredRows(filteredRows);
        event->Record.SetQueuedBytes(queuedBytes);
        event->Record.SetQueuedRows(queuedRows);
        LWPROBE(Statistics, consumer->ReadActorId.ToString(), consumer->QueryId, consumer->Generation, event->Record.ByteSizeLong());
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
    const auto& stat = ev->Get()->Stat;
    const auto& key = stat.SessionKey;

    LWPROBE(SessionStatistic, 
                ev->Sender.ToString(),
                key.ReadGroup,
                key.Endpoint,
                key.Database,
                key.TopicPath,
                key.PartitionId,
                stat.Common.ReadBytes,
                stat.Common.QueuedBytes,
                stat.Common.RestartSessionByOffsets,
                stat.Common.ReadEvents,
                stat.Common.LastReadedOffset);
    TTopicSessionKey sessionKey{key.ReadGroup, key.Endpoint, key.Database, key.TopicPath, key.PartitionId};

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
        auto partitionIt = consumerInfoPtr->Partitions.find(key.PartitionId);
        if (partitionIt == consumerInfoPtr->Partitions.end()) {
            continue;
        }
        partitionIt->second.Stat.Add(clientStat);
        partitionIt->second.FilteredBytes += clientStat.FilteredBytes;
        partitionIt->second.StatisticsUpdated = true;
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetInternalStateResponse::TPtr& ev) {
    LWPROBE(GetInternalState, ev->Sender.ToString(), ev->Get()->Record.ByteSizeLong());
    auto& readActorInternalState = ReadActorsInternalState[ev->Sender];
    readActorInternalState.InternalState = ev->Get()->Record.GetInternalState();
    readActorInternalState.ResponseTime = TInstant::Now();
}

void TRowDispatcher::UpdateCpuTime() {
    if (Consumers.empty()) {
        return;
    }
    auto currentCpuTime = UserPoolMetrics.Session->Val()
        + UserPoolMetrics.RowDispatcher->Val()
        + UserPoolMetrics.CompilerActor->Val()
        + UserPoolMetrics.CompilerService->Val()
        + UserPoolMetrics.FormatHandler->Val();
    auto diff = (currentCpuTime - LastCpuTime) / Consumers.size();
    for (auto& [actorId, consumer] : Consumers) {
        consumer->CpuMicrosec += diff;
    }
    LastCpuTime = currentCpuTime;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const TRowDispatcherSettings& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    NYdb::TDriver driver,
    NActors::TMon* monitoring,
    NActors::TActorId nodesManagerId,
    bool enableStreamingQueriesCounters)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
        config,
        credentialsProviderFactory,
        credentialsFactory,
        tenant,
        actorFactory,
        functionRegistry,
        counters,
        countersRoot,
        pqGateway,
        driver,
        monitoring,
        nodesManagerId,
        enableStreamingQueriesCounters));
}

} // namespace NFq
