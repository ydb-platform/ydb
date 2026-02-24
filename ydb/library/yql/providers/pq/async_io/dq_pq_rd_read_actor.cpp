#include "dq_pq_rd_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/pq_partition_key.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/join.h>
#include <ydb/library/actors/core/interconnect.h>

#include <queue>
#include <variant>

#define SRC_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

} // namespace

struct TRowDispatcherReadActorMetrics {
    explicit TRowDispatcherReadActorMetrics(const TTxId& txId, ui64 taskId, const ::NMonitoring::TDynamicCounterPtr& counters, const NPq::NProto::TDqPqTopicSource& sourceParams, bool enableStreamingQueriesCounters)
        : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
        , Counters(counters) {
        if (Counters) {
            SubGroup = Counters->GetSubgroup("source", "RdPqRead");
        } else {
            SubGroup = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        }

        auto task = SubGroup;
        if (enableStreamingQueriesCounters) {
            for (const auto& sensor : sourceParams.GetTaskSensorLabel()) {
                SubGroup = SubGroup->GetSubgroup(sensor.GetLabel(), sensor.GetValue());
            }
            Source = SubGroup->GetSubgroup("tx_id", TxId);
            task = Source->GetSubgroup("task_id", ToString(taskId));
        }
        InFlyGetNextBatch = task->GetCounter("InFlyGetNextBatch");
        InFlyAsyncInputData = task->GetCounter("InFlyAsyncInputData");
        ReInit = task->GetCounter("ReInit", true);
    }

    ~TRowDispatcherReadActorMetrics() {
        if (!Counters) {
            return;
        }
        SubGroup->RemoveSubgroup("tx_id", TxId);
    }

    TString TxId;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounterPtr Source;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyGetNextBatch;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReInit;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPrintState = EvBegin + 20,
        EvProcessState = EvBegin + 21,
        EvNotifyCA = EvBegin + 22,
        EvRefreshClusters = EvBegin + 23,
        EvReceivedClusters = EvBegin + 24,
        EvDescribeTopicResult = EvBegin + 25,
        EvPartitionIdleness = EvBegin + 26,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
    struct TEvProcessState : public NActors::TEventLocal<TEvProcessState, EvProcessState> {};
    struct TEvNotifyCA : public NActors::TEventLocal<TEvNotifyCA, EvNotifyCA> {};
    struct TEvPartitionIdleness : public NActors::TEventLocal<TEvPartitionIdleness, EvPartitionIdleness> {
        explicit TEvPartitionIdleness(TInstant notifyTime)
            : NotifyTime(notifyTime)
        {}
        const TInstant NotifyTime;
    };
    struct TEvRefreshClusters : public NActors::TEventLocal<TEvRefreshClusters, EvRefreshClusters> {};
    struct TEvReceivedClusters : public NActors::TEventLocal<TEvReceivedClusters, EvReceivedClusters> {
        explicit TEvReceivedClusters(
            std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>&& federatedClusters)
            : FederatedClusters(std::move(federatedClusters))
        {}
        explicit TEvReceivedClusters(const std::exception& ex)
            : ExceptionMessage(ex.what())
        {}
        std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> FederatedClusters;
        std::optional<std::string> ExceptionMessage;
    };
    struct TEvDescribeTopicResult : public NActors::TEventLocal<TEvDescribeTopicResult, EvDescribeTopicResult> {
        TEvDescribeTopicResult(ui32 clusterIndex, ui32 partitionsCount)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(partitionsCount)
        {}
        TEvDescribeTopicResult(ui32 clusterIndex, const NYdb::TStatus& status)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(0)
            , Status(status)
        {}
        ui32 ClusterIndex;
        ui32 PartitionsCount;
        TMaybe<NYdb::TStatus> Status;
    };
};

class TDqPqRdReadActor : public NActors::TActor<TDqPqRdReadActor>, public NYql::NDq::NInternal::TDqPqReadActorBase {
    static constexpr bool StaticDiscovery = true;

    const ui64 PrintStatePeriodSec = 300;
    const ui64 ProcessStatePeriodSec = 1;
    const ui64 PrintStateToLogSplitSize = 64000;
    const ui64 NotifyCAPeriodSec = 10;

    struct TReadyBatch {
    public:
        TReadyBatch(const TPartitionKey& partitionKey, TMaybe<TInstant> watermark, ui32 dataCapacity)
            : PartitionKey(partitionKey)
            , Watermark(watermark)
        {
            Data.reserve(dataCapacity);
        }

    public:
        TPartitionKey PartitionKey;
        TMaybe<TInstant> Watermark;
        TVector<TRope> Data;
        i64 UsedSpace = 0;
        ui64 NextOffset = 0;
    };

    enum class EState {
        START_CLUSTER_DISCOVERY,
        WAIT_CLUSTER_DISCOVERY,
        INIT,
        WAIT_COORDINATOR_ID,
        WAIT_PARTITIONS_ADDRES,
        STARTED
    };

    struct TCounters {
        ui64 GetAsyncInputData = 0;
        ui64 CoordinatorChanged = 0;
        ui64 CoordinatorResult = 0;
        ui64 MessageBatch = 0;
        ui64 StartSessionAck = 0;
        ui64 NewDataArrived = 0;
        ui64 SessionError = 0;
        ui64 Statistics = 0;
        ui64 NodeDisconnected = 0;
        ui64 NodeConnected = 0;
        ui64 Undelivered = 0;
        ui64 Retry = 0;
        ui64 PrivateHeartbeat = 0;
        ui64 SessionClosed = 0;
        ui64 Pong = 0;
        ui64 Heartbeat = 0;
        ui64 PrintState = 0;
        ui64 ProcessState = 0;
        ui64 NotifyCA = 0;
    };

private:
    // There can be 2 kinds of actors:
    // 1) Main (that CA interacts with)
    // 2) Child (per-federated-cluster, interacts with row dispatcher)
    // For single-cluster/non-federated topics main actor plays both roles.
    // PartitionToOffset maintained only on main actor, row dispatcher state maintained only on children
    // As main and child actors resides on same mailbox, their handlers are never concurrently executed
    // and can modify other side state
    TDqPqRdReadActor* Parent;
    TString Cluster;
    const TString Token;
    TMaybe<NActors::TActorId> CoordinatorActorId;
    NActors::TActorId LocalRowDispatcherActorId;
    // Set on Children
    std::queue<TReadyBatch> ReadyBuffer;
    // Set on Parent
    EState State = EState::INIT;
    bool Inited = false;
    ui64 CoordinatorRequestCookie = 0;
    TRowDispatcherReadActorMetrics Metrics;
    // Set on Parent
    bool ProcessStateScheduled = false;
    bool InFlyAsyncInputData = false;
    // Set on Parent
    TCounters Counters;
    // Set on Child (except for NotifyCA)
    // Parsing info
    std::vector<std::optional<ui64>> ColumnIndexes;  // Output column index in schema passed into RowDispatcher
    const TType* InputDataType = nullptr;  // Multi type (comes from Row Dispatcher)
    std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> DataUnpacker;
    // Set on Parent
    ui64 CpuMicrosec = 0;
    // Set on both Parent (cumulative) and Children (separate)

    using TPartitionKey = ::NPq::TPartitionKey;
    THashMap<ui64, ui64> NextOffsetFromRD;
    // Set on Children
    struct TClusterState {
        TClusterState(NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo&& info, ui32 partitionsCount)
            : Info(std::move(info))
            , PartitionsCount(partitionsCount)
        {}
        NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo Info;
        ITopicClient::TPtr TopicClient;
        ui32 PartitionsCount;
        TDqPqRdReadActor* Child = nullptr;
        NActors::TActorId ChildId;
    };
    std::vector<TClusterState> Clusters;
    // Set on Parent
    struct TSession {
        enum class ESessionStatus {
            INIT,
            WAIT_START_SESSION_ACK,
            STARTED,
        };

        TSession(
            const TTxId& txId,
            const NActors::TActorId selfId,
            TActorId rowDispatcherActorId,
            ui64 eventQueueId,
            ui64 generation)
            : TxId(txId)
            , SelfId(selfId)
            , EventQueueId(eventQueueId)
            , RowDispatcherActorId(rowDispatcherActorId)
            , Generation(generation)
        {
            EventsQueue.Init(TxId, SelfId, SelfId, EventQueueId, /* KeepAlive */ true);
            EventsQueue.OnNewRecipientId(rowDispatcherActorId);
        }

        ESessionStatus Status = ESessionStatus::INIT;
        const TTxId TxId;
        const NActors::TActorId SelfId;
        const ui64 EventQueueId;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        TActorId RowDispatcherActorId;
        ui64 Generation = std::numeric_limits<ui64>::max();
        THashSet<ui32> HasPendingData;
        THashSet<ui32> Partitions;

        bool IsWaitingStartSessionAck = false;
        ui64 QueuedBytes = 0;
        ui64 QueuedRows = 0;
    };

    IPqGateway::TPtr PqGateway;
    TMap<NActors::TActorId, TSession> Sessions;
    THashMap<ui64, NActors::TActorId> ReadActorByEventQueueId;
    // Set on Children
    const THolderFactory& HolderFactory;
    const TTypeEnvironment& TypeEnv;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    IFederatedTopicClient::TPtr FederatedTopicClient;
    const i64 MaxBufferSize;
    i64 ReadyBufferSizeBytes = 0;
    // Set on Parent
    ui64 NextGeneration = 0;
    ui64 NextEventQueueId = 0;
    bool EnableStreamingQueriesCounters = false;

    TMap<NActors::TActorId, TSet<ui32>> LastUsedPartitionDistribution;
    TMap<NActors::TActorId, TSet<ui32>> LastReceivedPartitionDistribution;
    // Set on Children

public:
    TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        const TTypeEnvironment& typeEnv,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& localRowDispatcherActorId,
        const TString& token,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize,
        const IPqGateway::TPtr& pqGateway,
        bool enableStreamingQueriesCounters,
        TDqPqRdReadActor* parent = nullptr,
        const TString& cluster = {});

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStatistics::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetInternalStateRequest::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorDistributionReset::TPtr& ev);

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr&);
    void Handle(NActors::TEvents::TEvPong::TPtr& ev);
    void Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr&);
    void Handle(TEvPrivate::TEvPrintState::TPtr&);
    void Handle(TEvPrivate::TEvProcessState::TPtr&);
    void Handle(TEvPrivate::TEvNotifyCA::TPtr&);
    void Handle(TEvPrivate::TEvPartitionIdleness::TPtr&);
    void Handle(TEvPrivate::TEvRefreshClusters::TPtr&);
    void Handle(TEvPrivate::TEvReceivedClusters::TPtr&);
    void Handle(TEvPrivate::TEvDescribeTopicResult::TPtr&);

    STRICT_STFUNC(StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorResult, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSessionAck, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStatistics, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetInternalStateRequest, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorDistributionReset, Handle);

        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, Handle);
        hFunc(TEvPrivate::TEvPrintState, Handle);
        hFunc(TEvPrivate::TEvProcessState, Handle);
        hFunc(TEvPrivate::TEvNotifyCA, Handle);
        hFunc(TEvPrivate::TEvPartitionIdleness, Handle);
        hFunc(TEvPrivate::TEvRefreshClusters, Handle);
        hFunc(TEvPrivate::TEvReceivedClusters, Handle);
        hFunc(TEvPrivate::TEvDescribeTopicResult, Handle);
    })

    STRICT_STFUNC(IgnoreState, {
        // ignore all events except for retry queue
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, IgnoreEvent);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorResult, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvStartSessionAck, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvStatistics, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvGetInternalStateRequest, ReplyNoSession);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorDistributionReset, Handle);

        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);

        // ignore all row dispatcher events
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, ReplyNoSession);
        hFunc(TEvPrivate::TEvPrintState, IgnoreEvent);
        hFunc(TEvPrivate::TEvProcessState, IgnoreEvent);
        hFunc(TEvPrivate::TEvNotifyCA, IgnoreEvent);
        hFunc(TEvPrivate::TEvPartitionIdleness, IgnoreEvent);
        hFunc(TEvPrivate::TEvRefreshClusters, IgnoreEvent);
        hFunc(TEvPrivate::TEvReceivedClusters, IgnoreEvent);
        hFunc(TEvPrivate::TEvDescribeTopicResult, IgnoreEvent);
    })

    template <class TEventPtr>
    void IgnoreEvent(TEventPtr& ev) {
        SRC_LOG_D("Ignore " << typeid(TEventPtr).name() << " from " << ev->Sender);
    }

    template <class TEventPtr>
    void ReplyNoSession(TEventPtr& ev) {
        SRC_LOG_D("Ignore (no session) " << typeid(TEventPtr).name() << " from " << ev->Sender);
        SendNoSession(ev->Sender, ev->Cookie);
    }

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    TDuration GetCpuTime() override;
    std::vector<ui64> GetPartitionsToRead() const;
    void AddMessageBatch(TRope&& serializedBatch, NKikimr::NMiniKQL::TUnboxedValueBatch& buffer);
    void ProcessState();
    void StopSession(TSession& sessionInfo);
    void Stop(NDqProto::StatusIds::StatusCode status, TIssues issues);
    void ReInit(const TString& reason);
    void PrintInternalState();
    void TrySendGetNextBatch(TSession& sessionInfo);
    TString GetInternalState();
    template <class TEventPtr>
    TSession* FindAndUpdateSession(const TEventPtr& ev);
    void SendNoSession(const NActors::TActorId& recipient, ui64 cookie);
    void NotifyCA();
    void SchedulePartitionIdlenessCheck(TInstant) override;
    void InitWatermarkTracker() override;
    void SendStartSession(TSession& sessionInfo);
    void Init();
    void InitChild();
    void ScheduleProcessState();
    void ProcessGlobalState();
    void ProcessSessionsState();
    void UpdateSessions();
    void UpdateQueuedSize();
    void StartClusterDiscovery();
    void StartCluster(ui32 clusterIndex);
    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const;
    IFederatedTopicClient& GetFederatedTopicClient();
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const;
    ITopicClient& GetTopicClient(TClusterState& clusterState);
};

IFederatedTopicClient& TDqPqRdReadActor::GetFederatedTopicClient() {
    if (!FederatedTopicClient) {
        FederatedTopicClient = PqGateway->GetFederatedTopicClient(Driver, GetFederatedTopicClientSettings());
    }
    return *FederatedTopicClient;
}

ITopicClient& TDqPqRdReadActor::GetTopicClient(TClusterState& clusterState) {
    if (!clusterState.TopicClient) {
        auto settings = GetTopicClientSettings();
        clusterState.Info.AdjustTopicClientSettings(settings);
        clusterState.TopicClient = PqGateway->GetTopicClient(Driver, settings);
    }
    return *clusterState.TopicClient;
}

NYdb::NFederatedTopic::TFederatedTopicClientSettings TDqPqRdReadActor::GetFederatedTopicClientSettings() const {
    NYdb::NFederatedTopic::TFederatedTopicClientSettings opts = PqGateway->GetFederatedTopicClientSettings();
    opts.Database(SourceParams.GetDatabase())
        .DiscoveryEndpoint(SourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);

    return opts;
}

NYdb::NTopic::TTopicClientSettings TDqPqRdReadActor::GetTopicClientSettings() const {
    NYdb::NTopic::TTopicClientSettings opts = PqGateway->GetTopicClientSettings();
    opts.Database(SourceParams.GetDatabase())
        .DiscoveryEndpoint(SourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);

    return opts;
}

TDqPqRdReadActor::TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        const TTypeEnvironment& typeEnv,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& localRowDispatcherActorId,
        const TString& token,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize,
        const IPqGateway::TPtr& pqGateway,
        bool enableStreamingQueriesCounters,
        TDqPqRdReadActor* parent,
        const TString& cluster)
        : TActor<TDqPqRdReadActor>(&TDqPqRdReadActor::StateFunc)
        , TDqPqReadActorBase(inputIndex, taskId, this->SelfId(), txId, std::move(sourceParams), std::move(readParams), computeActorId)
        , Parent(parent ? parent : this)
        , Cluster(cluster)
        , Token(token)
        , LocalRowDispatcherActorId(localRowDispatcherActorId)
        , Metrics(txId, taskId, counters, SourceParams, enableStreamingQueriesCounters)
        , PqGateway(pqGateway)
        , HolderFactory(holderFactory)
        , TypeEnv(typeEnv)
        , Driver(std::move(driver))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , MaxBufferSize(bufferSize)
        , EnableStreamingQueriesCounters(enableStreamingQueriesCounters)
{
    SRC_LOG_I("Start read actor, local row dispatcher " << LocalRowDispatcherActorId.ToString() << ", metadatafields: " << JoinSeq(',', SourceParams.GetMetadataFields())
        << ", partitions: " << JoinSeq(',', GetPartitionsToRead()) << ", skip json errors: " << SourceParams.GetSkipJsonErrors());
    if (Parent != this) {
        return;
    }
    State = EState::START_CLUSTER_DISCOVERY;
    const auto programBuilder = std::make_unique<TProgramBuilder>(typeEnv, *holderFactory.GetFunctionRegistry());

    // Parse output schema (expected struct output type)
    const TStringBuf outputTypeYson(SourceParams.GetRowType());
    TStringStream error;
    const auto outputItemType = NCommon::ParseTypeFromYson(outputTypeYson, *programBuilder, error);
    YQL_ENSURE(outputItemType, "Failed to parse output type: " << outputTypeYson << ", reason: " << error.Str());
    YQL_ENSURE(outputItemType->IsStruct(), "Output type " << outputTypeYson << " is not struct");
    const auto structType = static_cast<TStructType*>(outputItemType);

    // Build input schema and unpacker (for data comes from RowDispatcher)
    TVector<TType* const> inputTypeParts;
    inputTypeParts.reserve(SourceParams.ColumnsSize());
    ColumnIndexes.resize(structType->GetMembersCount());
    for (size_t i = 0; i < SourceParams.ColumnsSize(); ++i) {
        const auto index = structType->GetMemberIndex(SourceParams.GetColumns().Get(i));
        inputTypeParts.emplace_back(structType->GetMemberType(index));
        ColumnIndexes[index] = i;
    }
    InputDataType = programBuilder->NewMultiType(inputTypeParts);
    DataUnpacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(InputDataType, NKikimr::NMiniKQL::EValuePackerVersion::V0);

    InitWatermarkTracker(); // non-virtual!
    IngressStats.Level = statsLevel;
}

void TDqPqRdReadActor::Init() {
    if (Inited) {
        return;
    }
    LogPrefix = (TStringBuilder() << "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ", Cluster: " << Cluster << ". PQ source. ");

    Inited = true;

    if (Parent == this) {
        ProcessState();
        Schedule(TDuration::Seconds(NotifyCAPeriodSec), new TEvPrivate::TEvNotifyCA());
    }
}

void TDqPqRdReadActor::InitChild() {
    for (auto& [partitionKey, offset]: Parent->PartitionToOffset) {
        if (Cluster == partitionKey.Cluster) {
            NextOffsetFromRD[partitionKey.PartitionId] = offset;
        }
    }
    StartingMessageTimestamp = Parent->StartingMessageTimestamp;
    SRC_LOG_I("Send TEvCoordinatorChangesSubscribe to local RD (" << LocalRowDispatcherActorId << ")");
    Send(LocalRowDispatcherActorId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
    // Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());   // Logs (InternalState) is too big
}

void TDqPqRdReadActor::ProcessGlobalState() {
    switch (State) {
    case EState::START_CLUSTER_DISCOVERY:
        State = EState::WAIT_CLUSTER_DISCOVERY;
        StartClusterDiscovery();
        break;

    case EState::WAIT_CLUSTER_DISCOVERY:
        for (auto& clusterState : Clusters) {
            auto child = clusterState.Child;
            if (child == nullptr) {
                continue;
            }
            child->ProcessState();
        }
        break;

    case EState::INIT:
        if (!Parent->ReadyBuffer.empty()) {
            return;
        }
        if (!CoordinatorActorId) {
            SRC_LOG_I("Send TEvCoordinatorChangesSubscribe to local row dispatcher, self id " << SelfId());
            Send(LocalRowDispatcherActorId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
            State = EState::WAIT_COORDINATOR_ID;
        }
        [[fallthrough]];
    case EState::WAIT_COORDINATOR_ID: {
        if (!CoordinatorActorId) {
            return;
        }
        auto partitionToRead = GetPartitionsToRead();
        if (WatermarkTracker) {
            auto now = TInstant::Now();
            TPartitionKey partitionKey { .Cluster = Cluster };
            for (auto partitionId: partitionToRead) {
                partitionKey.PartitionId = partitionId;
                WatermarkTracker->RegisterPartition(partitionKey, now);
            }
        }
        auto cookie = ++CoordinatorRequestCookie;
        SRC_LOG_I("Send TEvCoordinatorRequest to coordinator " << CoordinatorActorId->ToString() << ", partIds: "
            << JoinSeq(", ", partitionToRead) << " cookie " << cookie);
        Send(
            *CoordinatorActorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorRequest(SourceParams, partitionToRead),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            cookie);
        LastReceivedPartitionDistribution.clear();
        State = EState::WAIT_PARTITIONS_ADDRES;
        [[fallthrough]];
    }
    case EState::WAIT_PARTITIONS_ADDRES:
        if (LastReceivedPartitionDistribution.empty()) {
            break;
        }
        UpdateSessions();
        State = EState::STARTED;
        [[fallthrough]];
    case EState::STARTED:
        break;
    }
}

void TDqPqRdReadActor::ProcessSessionsState() {
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        switch (sessionInfo.Status) {
        case TSession::ESessionStatus::INIT:
            SendStartSession(sessionInfo);
            sessionInfo.IsWaitingStartSessionAck = true;
            sessionInfo.Status = TSession::ESessionStatus::WAIT_START_SESSION_ACK;
            [[fallthrough]];
        case TSession::ESessionStatus::WAIT_START_SESSION_ACK:
            if (sessionInfo.IsWaitingStartSessionAck) {
                break;
            }
            sessionInfo.Status = TSession::ESessionStatus::STARTED;
            [[fallthrough]];
        case TSession::ESessionStatus::STARTED:
            break;
        }
    }
}

void TDqPqRdReadActor::ProcessState() {
    ProcessGlobalState();
    ProcessSessionsState();
}

void TDqPqRdReadActor::SendStartSession(TSession& sessionInfo) {
    auto str = TStringBuilder() << "Send TEvStartSession to " << sessionInfo.RowDispatcherActorId
        << ", connection id " << sessionInfo.Generation << " partitions offsets ";

    std::set<ui32> partitions;
    std::map<ui32, ui64> partitionOffsets;
    for (auto partitionId : sessionInfo.Partitions) {
        partitions.insert(partitionId);
        auto itNextOffset = NextOffsetFromRD.find(partitionId);
        str << "(" << partitionId << " / ";
        if (itNextOffset == NextOffsetFromRD.end()) {
            str << "<empty>),";
            continue;
        }
        partitionOffsets[partitionId] = itNextOffset->second;
        str << itNextOffset->second << "),";
    }
    SRC_LOG_I(str);

    auto event = new NFq::TEvRowDispatcher::TEvStartSession(
        SourceParams,
        partitions,
        Token,
        partitionOffsets,
        StartingMessageTimestamp.MilliSeconds(),
        std::visit([](auto arg) { return ToString(arg); }, TxId));
    sessionInfo.EventsQueue.Send(event, sessionInfo.Generation);
}

void TDqPqRdReadActor::CommitState(const NDqProto::TCheckpoint& /*checkpoint*/) {
}

void TDqPqRdReadActor::StopSession(TSession& sessionInfo) {
    SRC_LOG_I("Send StopSession to " << sessionInfo.RowDispatcherActorId
        << " generation " << sessionInfo.Generation);
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    *event->Record.MutableSource() = SourceParams;
    sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor
    SRC_LOG_I("PassAway");
    Become(&TDqPqRdReadActor::IgnoreState);
    PrintInternalState();
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        StopSession(sessionInfo);
    }
    for (auto& clusterState : Clusters) {
        auto child = clusterState.Child;
        if (child == this) {
            continue;
        }
        // all actors are on same mailbox, safe to call
        child->PassAway();
    }
    Clusters.clear();
    FederatedTopicClient.Reset();
    TActor<TDqPqRdReadActor>::PassAway();

    // TODO: RetryQueue::Unsubscribe()
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool& /* finished */, i64 freeSpace) {
    Counters.GetAsyncInputData++;
    SRC_LOG_T("GetAsyncInputData freeSpace = " << freeSpace);
    Init();
    Metrics.InFlyAsyncInputData->Set(0);
    InFlyAsyncInputData = false;
    buffer.clear();
    watermark = Nothing();

    if (WatermarkTracker) {
        const auto now = TInstant::Now();
        const auto idleWatermark = WatermarkTracker->HandleIdleness(now);

        if (idleWatermark) {
            SRC_LOG_D("SessionId: " << GetSessionId() << " Idleness watermark " << idleWatermark << " was produced");
            if (ReadyBuffer.empty()) {
                watermark = *idleWatermark;
            } else {
                Y_ENSURE(ReadyBuffer.back().Watermark < *idleWatermark);
                ReadyBuffer.back().Watermark = *idleWatermark;
            }
        }
        MaybeSchedulePartitionIdlenessCheck(now);
    }

    if (freeSpace == 0 || ReadyBuffer.empty()) {
        return 0;
    }

    i64 usedSpace = 0;
    do {
        auto readyBatch = std::move(ReadyBuffer.front());
        ReadyBuffer.pop();

        for (auto& messageBatch : readyBatch.Data) {
            AddMessageBatch(std::move(messageBatch), buffer);
        }

        watermark = readyBatch.Watermark;
        usedSpace += readyBatch.UsedSpace;
        freeSpace -= readyBatch.UsedSpace;
        PartitionToOffset[readyBatch.PartitionKey] = readyBatch.NextOffset;
        SRC_LOG_T("NextOffset " << readyBatch.NextOffset);
    } while (freeSpace > 0 && !ReadyBuffer.empty() && watermark.Empty());

    ReadyBufferSizeBytes -= usedSpace;
    SRC_LOG_T("Return " << buffer.RowCount() << " rows, watermark " << watermark << ", buffer size " << ReadyBufferSizeBytes << ", free space " << freeSpace << ", result size " << usedSpace);

    if (!ReadyBuffer.empty()) {
        NotifyCA();
    }
    for (auto& clusterState : Clusters) {
        auto child = clusterState.Child;
        if (child == nullptr) {
            continue;
        }
        for (auto& [rowDispatcherActorId, sessionInfo] : child->Sessions) {
            // all actors are on same mailbox, safe to call
            child->TrySendGetNextBatch(sessionInfo);
        }
    }
    return usedSpace;
}

TDuration TDqPqRdReadActor::GetCpuTime() {
    return TDuration::MicroSeconds(CpuMicrosec);
}

void TDqPqRdReadActor::SchedulePartitionIdlenessCheck(TInstant at) {
    Schedule(at, new TEvPrivate::TEvPartitionIdleness(at));
}

void TDqPqRdReadActor::InitWatermarkTracker() {
    Y_DEBUG_ABORT_UNLESS(Parent == this); // called on Parent
    auto lateArrivalDelayUs = SourceParams.GetWatermarks().GetLateArrivalDelayUs();
    auto idleTimeoutUs = // TODO remove fallback
        SourceParams.GetWatermarks().HasIdleTimeoutUs() ?
        SourceParams.GetWatermarks().GetIdleTimeoutUs() :
        lateArrivalDelayUs;
    TDqPqReadActorBase::InitWatermarkTracker(
            TDuration::Zero(), // lateArrivalDelay is embedded into calculation of WatermarkExpr
            TDuration::MicroSeconds(idleTimeoutUs),
            Metrics.Counters ? Metrics.Source : nullptr);
}

std::vector<ui64> TDqPqRdReadActor::GetPartitionsToRead() const {
    std::vector<ui64> res;

    for (const auto& readParams : ReadParams) {
        ui32 partitionsCount = readParams.GetPartitioningParams().GetTopicPartitionsCount();
        ui64 currentPartition = readParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
        do {
            res.emplace_back(currentPartition); // 0-based in topic API
            currentPartition += readParams.GetPartitioningParams().GetDqPartitionsCount();
        } while (currentPartition < partitionsCount);
    }
    return res;
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_I("Received TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() <<  ", generation " << ev->Cookie);
    Counters.StartSessionAck++;
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    session->IsWaitingStartSessionAck = false;
    ProcessState();
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_I("Received TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.SessionError++;

    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    NYql::TIssues issues;
    IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
    Stop(ev->Get()->Record.GetStatusCode(), issues);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStatistics::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvStatistics from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.Statistics++;
    CpuMicrosec += ev->Get()->Record.GetCpuMicrosec();
    // all actors are on same mailbox, this method is not called after Parent stopped, safe to access directly
    if (Parent != this) {
        Parent->CpuMicrosec += ev->Get()->Record.GetCpuMicrosec();
    }
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    Parent->IngressStats.Bytes += ev->Get()->Record.GetReadBytes();
    Parent->IngressStats.FilteredBytes += ev->Get()->Record.GetFilteredBytes();
    Parent->IngressStats.FilteredRows += ev->Get()->Record.GetFilteredRows();
    session->QueuedBytes = ev->Get()->Record.GetQueuedBytes();
    session->QueuedRows = ev->Get()->Record.GetQueuedRows();
    UpdateQueuedSize();

    for (auto partition : ev->Get()->Record.GetPartition()) {
        auto partitionId = partition.GetPartitionId();
        if (!partition.HasNextMessageOffset()) {
            continue;
        }
        auto offset = partition.GetNextMessageOffset();
        auto [itNextOffset, inserted] = NextOffsetFromRD.emplace(partitionId, offset);
        if (!inserted) {
            itNextOffset->second = std::max(itNextOffset->second, offset);
        }
        SRC_LOG_T("NextOffsetFromRD [" << partitionId << "]= " << itNextOffset->second);
        if (Parent->ReadyBuffer.empty()) {
            auto partitionKey = TPartitionKey { Cluster, partitionId };
            Parent->PartitionToOffset[partitionKey] = itNextOffset->second;
        }
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvGetInternalStateRequest::TPtr& ev) {
    auto response = std::make_unique<NFq::TEvRowDispatcher::TEvGetInternalStateResponse>();
    response->Record.SetInternalState(GetInternalState());
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorDistributionReset::TPtr& ev) {
    if (CoordinatorActorId != ev->Sender) {
        SRC_LOG_I("Ignore TEvCoordinatorDistributionReset, sender is not active coordinator (sender " << ev->Sender << ", current coordinator " << CoordinatorActorId << ")");
        return;
    }
    SRC_LOG_I("Received TEvCoordinatorDistributionReset from " << ev->Sender);
    ReInit("Distribution changed");
    ScheduleProcessState();
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {
    auto partitionId = ev->Get()->Record.GetPartitionId();
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvNewDataArrived from " << ev->Sender << ", partition " << partitionId << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.NewDataArrived++;

    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    auto partitionIt = session->Partitions.find(partitionId);
    if (partitionIt == session->Partitions.end()) {
        SRC_LOG_E("Received TEvNewDataArrived from " << ev->Sender << " with wrong partition id " << partitionId);
        Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "No partition with id " << partitionId )});
        return;
    }
    session->HasPendingData.insert(partitionId);
    TrySendGetNextBatch(*session);
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    SRC_LOG_T("Received TEvRetry, EventQueueId " << ev->Get()->EventQueueId);
    Counters.Retry++;

    auto readActorIt = ReadActorByEventQueueId.find(ev->Get()->EventQueueId);
    if (readActorIt == ReadActorByEventQueueId.end()) {
        SRC_LOG_D("Ignore TEvRetry, wrong EventQueueId " << ev->Get()->EventQueueId);
        return;
    }

    auto sessionIt = Sessions.find(readActorIt->second);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_D("Ignore TEvRetry, wrong read actor id " << readActorIt->second);
        return;
    }
    sessionIt->second.EventsQueue.Retry();
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("TEvRetryQueuePrivate::TEvEvHeartbeat");
    Counters.PrivateHeartbeat++;
    auto readActorIt = ReadActorByEventQueueId.find(ev->Get()->EventQueueId);
    if (readActorIt == ReadActorByEventQueueId.end()) {
        SRC_LOG_D("Ignore TEvEvHeartbeat, wrong EventQueueId " << ev->Get()->EventQueueId);
        return;
    }

    auto sessionIt = Sessions.find(readActorIt->second);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_D("Ignore TEvEvHeartbeat, wrong read actor id " << readActorIt->second);
        return;
    }
    auto& sessionInfo = sessionIt->second;
    bool needSend = sessionInfo.EventsQueue.Heartbeat();
    if (needSend) {
        SRC_LOG_T("Send TEvEvHeartbeat");
        sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(), sessionInfo.Generation);
    }
}

void TDqPqRdReadActor::Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("Received TEvHeartbeat from " << ev->Sender << ", generation " << ev->Cookie);
    Counters.Heartbeat++;
    FindAndUpdateSession(ev);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    SRC_LOG_D("TEvCoordinatorChanged, new coordinator " << ev->Get()->CoordinatorActorId);
    Counters.CoordinatorChanged++;

    if (CoordinatorActorId
        && CoordinatorActorId == ev->Get()->CoordinatorActorId) {
        return;
    }

    if (!CoordinatorActorId) {
        CoordinatorActorId = ev->Get()->CoordinatorActorId;
        ProcessState();
        return;
    }

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    ReInit("Coordinator is changed");
    ScheduleProcessState();
}

void TDqPqRdReadActor::ScheduleProcessState() {
    if (ProcessStateScheduled) {
        return;
    }
    ProcessStateScheduled = true;
    Schedule(TDuration::Seconds(ProcessStatePeriodSec), new TEvPrivate::TEvProcessState());
}

void TDqPqRdReadActor::ReInit(const TString& reason) {
    SRC_LOG_I("ReInit state, reason " << reason);
    // all actors are on same mailbox, this method is not called after Parent stopped, safe to access directly
    Parent->Metrics.ReInit->Inc();

    State = EState::WAIT_COORDINATOR_ID;
    if (!Parent->ReadyBuffer.empty()) {
        Parent->NotifyCA();
    }
}

void TDqPqRdReadActor::Stop(NDqProto::StatusIds::StatusCode status, TIssues issues) {
    SRC_LOG_E("Stop read actor, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(status) << ", issues: " << issues.ToOneLineString());
    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), status));
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev) {
    SRC_LOG_I("Received TEvCoordinatorResult from " << ev->Sender.ToString() << ", cookie " << ev->Cookie);
    Counters.CoordinatorResult++;
    if (ev->Cookie != CoordinatorRequestCookie) {
        SRC_LOG_W("Ignore TEvCoordinatorResult. wrong cookie");
        return;
    }
    if (!ev->Get()->Record.GetIssues().empty()) {
        NYql::TIssues issues;
        IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
        Stop(NYql::NDqProto::StatusIds::BAD_REQUEST, issues);
        return;
    }
    LastReceivedPartitionDistribution.clear();
    TMap<NActors::TActorId, TSet<ui32>> distribution;
    for (auto& p : ev->Get()->Record.GetPartitions()) {
        TActorId rowDispatcherActorId = ActorIdFromProto(p.GetActorId());
        // Note: for federated case all clusters are handled by same row dispatcher
        for (auto partitionId : p.GetPartitionIds()) {
            LastReceivedPartitionDistribution[rowDispatcherActorId].insert(partitionId);
        }
    }
    ProcessState();
}

void TDqPqRdReadActor::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
    Counters.NodeConnected++;
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TDqPqRdReadActor::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    SRC_LOG_D("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    Counters.NodeDisconnected++;
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    // In case of row dispatcher disconnection: wait connected or SessionClosed(). TODO: Stop actor after timeout.
    // In case of row dispatcher disconnection: wait CoordinatorChanged().
    //Stop(TString{"Node disconnected, nodeId "} + ToString(ev->Get()->NodeId));
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    SRC_LOG_D("Received TEvUndelivered, " << ev->Get()->ToString() << " from " << ev->Sender.ToString() << ", reason " << ev->Get()->Reason << ", cookie " << ev->Cookie);
    Counters.Undelivered++;

    auto sessionIt = Sessions.find(ev->Sender);
    if (sessionIt != Sessions.end()) {
        auto& sessionInfo = sessionIt->second;
        if (sessionInfo.EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            if (sessionInfo.Generation == ev->Cookie) {
                SRC_LOG_D("Erase session to " << ev->Sender.ToString());
                ReadActorByEventQueueId.erase(sessionInfo.EventQueueId);
                Sessions.erase(sessionIt);
                ReInit("Reset session state (by TEvUndelivered)");
                ScheduleProcessState();
            }
        }
    }

    if (CoordinatorActorId && *CoordinatorActorId == ev->Sender) {
        ReInit("TEvUndelivered to coordinator");
        ScheduleProcessState();
        return;
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.MessageBatch++;
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    auto partitionId = ev->Get()->Record.GetPartitionId();
    auto partitionIt = session->Partitions.find(partitionId);
    if (partitionIt == session->Partitions.end()) {
        SRC_LOG_E("TEvMessageBatch: wrong partition id " << partitionId);
        Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "No partition with id " << partitionId)});
        return;
    }
    // all actors are on same mailbox, this method is not called after Parent stopped, safe to access directly
    Parent->Metrics.InFlyGetNextBatch->Set(0);

    const auto& messages = ev->Get()->Record.GetMessages();
    if (messages.empty()) {
        return;
    }

    TPartitionKey partitionKey { Cluster, partitionId };

    if (Parent->ReadyBuffer.empty() || Parent->ReadyBuffer.back().PartitionKey != partitionKey) {
        Parent->ReadyBuffer.emplace(partitionKey, Nothing(), messages.size());
    }

    auto& nextOffset = NextOffsetFromRD[partitionId];

    for (const auto& message : messages) {
        if (Parent->ReadyBuffer.back().Watermark.Defined()) {
            Parent->ReadyBuffer.emplace(partitionKey, Nothing(), messages.size());
        }
        TReadyBatch& activeBatch = Parent->ReadyBuffer.back();

        const auto& offsets = message.GetOffsets();
        if (offsets.empty()) {
            Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "Got unexpected empty batch from row dispatcher")});
            return;
        }

        activeBatch.Data.emplace_back(ev->Get()->GetPayload(message.GetPayloadId()));
        auto size = activeBatch.Data.back().GetSize();
        activeBatch.UsedSpace += size;
        Parent->ReadyBufferSizeBytes += size;

        nextOffset = *offsets.rbegin() + 1;
        activeBatch.NextOffset = nextOffset;
        SRC_LOG_T("TEvMessageBatch NextOffset " << activeBatch.NextOffset);

        // Watermark processing
        const auto& watermarksUs = message.GetWatermarksUs();
        if (watermarksUs.empty() || !Parent->WatermarkTracker) {
            continue;
        }
        TMaybe<TInstant> newWatermark;
        for (auto watermarkUs : watermarksUs) {
            const auto watermark = TInstant::MicroSeconds(watermarkUs);
            const auto maybeNewWatermark = Parent->WatermarkTracker->NotifyNewPartitionTime(
                partitionKey,
                watermark,
                TInstant::Now()
            );
            if (!maybeNewWatermark) {
                continue;
            }
            newWatermark = *maybeNewWatermark;
        }

        if (newWatermark) {
            SRC_LOG_D("SessionId: " << GetSessionId() << " New watermark " << newWatermark << " was generated");
            activeBatch.Watermark = newWatermark;
        }
    }

    Parent->NotifyCA();
}

void TDqPqRdReadActor::AddMessageBatch(TRope&& messageBatch, NKikimr::NMiniKQL::TUnboxedValueBatch& buffer) {
    // TODO: pass multi type directly to CA, without transforming it into struct

    NKikimr::NMiniKQL::TUnboxedValueBatch parsedData(InputDataType);
    DataUnpacker->UnpackBatch(MakeChunkedBuffer(std::move(messageBatch)), HolderFactory, parsedData);

    while (!parsedData.empty()) {
        const auto* parsedRow = parsedData.Head();

        NUdf::TUnboxedValue* itemPtr;
        NUdf::TUnboxedValuePod item = HolderFactory.CreateDirectArrayHolder(ColumnIndexes.size(), itemPtr);
        for (const auto index : ColumnIndexes) {
            if (index) {
                YQL_ENSURE(*index < parsedData.Width(), "Unexpected data width " << parsedData.Width() << ", failed to extract column by index " << index);
                *(itemPtr++) = parsedRow[*index];
            } else {
                // TODO: support metadata fields here
                *(itemPtr++) = NUdf::TUnboxedValuePod::Zero();
            }
        }

        buffer.emplace_back(std::move(item));
        parsedData.Pop();
    }
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvPong::TPtr& ev) {
    SRC_LOG_T("TEvPong from " << ev->Sender);
    Counters.Pong++;
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvPrintState::TPtr&) {
    Counters.PrintState++;
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
    PrintInternalState();
}

void TDqPqRdReadActor::PrintInternalState() {
    auto str = GetInternalState();
    auto buf = TStringBuf(str);
    for (ui64 offset = 0; offset < buf.size(); offset += PrintStateToLogSplitSize) {
        SRC_LOG_I(buf.SubString(offset, PrintStateToLogSplitSize));
    }
}

TString TDqPqRdReadActor::GetInternalState() {
    TStringStream str;
    str << LogPrefix << "State:";
    str << " used buffer size " << Parent->ReadyBufferSizeBytes << " ready buffer event size " << Parent->ReadyBuffer.size()
        << " state " << static_cast<ui64>(State)
        << " InFlyAsyncInputData " << Parent->InFlyAsyncInputData
        << "\n";
    str << "Counters:"
        << " CoordinatorChanged " << Counters.CoordinatorChanged << " CoordinatorResult " << Counters.CoordinatorResult
        << " MessageBatch " << Counters.MessageBatch << " StartSessionAck " << Counters.StartSessionAck << " NewDataArrived " << Counters.NewDataArrived
        << " SessionError " << Counters.SessionError << " Statistics " << Counters.Statistics << " NodeDisconnected " << Counters.NodeDisconnected
        << " NodeConnected " << Counters.NodeConnected << " Undelivered " << Counters.Undelivered << " Retry " << Counters.Retry
        << " PrivateHeartbeat " << Counters.PrivateHeartbeat << " SessionClosed " << Counters.SessionClosed << " Pong " << Counters.Pong
        << " Heartbeat " << Counters.Heartbeat << " PrintState " << Counters.PrintState << " ProcessState " << Counters.ProcessState
        << " GetAsyncInputData " << Parent->Counters.GetAsyncInputData
        << " NotifyCA " << Parent->Counters.NotifyCA
        << "\n";

    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        str << " " << rowDispatcherActorId << " status " << static_cast<ui64>(sessionInfo.Status)
            << " is waiting ack " << sessionInfo.IsWaitingStartSessionAck << " connection id " << sessionInfo.Generation << " ";
        sessionInfo.EventsQueue.PrintInternalState(str);
        str << " partitions";
        for (const auto partitionId : sessionInfo.Partitions) {
            str << " " << partitionId;
        }
        str << " offsets";
        for (const auto& [partitionId, offset] : NextOffsetFromRD) {
            str << " " << partitionId << "=" << offset;
        }
        str << " has pending data";
        for (const auto partitionId : sessionInfo.HasPendingData) {
            str << " " << partitionId;
        }
        str << "\n";
    }
    if (Parent->WatermarkTracker) {
        str << "WatermarksTracker:";
        Parent->WatermarkTracker->Out(str);
        str << "\n";
    }
    return str.Str();
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvProcessState::TPtr&) {
    ProcessStateScheduled = false;
    Counters.ProcessState++;
    ProcessState();
}

void TDqPqRdReadActor::TrySendGetNextBatch(TSession& sessionInfo) {
    // called on child
    if (Parent->ReadyBufferSizeBytes > MaxBufferSize) {
        return;
    }
    for (auto partitionId : sessionInfo.HasPendingData) {
        Parent->Metrics.InFlyGetNextBatch->Inc();
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvGetNextBatch>();
        event->Record.SetPartitionId(partitionId);
        sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
    }
    sessionInfo.HasPendingData.clear();
}

template <class TEventPtr>
TDqPqRdReadActor::TSession* TDqPqRdReadActor::FindAndUpdateSession(const TEventPtr& ev) {
    auto sessionIt = Sessions.find(ev->Sender);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore " << typeid(TEventPtr).name() << " from " << ev->Sender);
        SendNoSession(ev->Sender, ev->Cookie);
        return nullptr;
    }
    auto& session = sessionIt->second;

    if (ev->Cookie != session.Generation) {
        SRC_LOG_W("Wrong message generation (" << typeid(TEventPtr).name() << "), sender " << ev->Sender << " cookie " << ev->Cookie << ", session generation " << session.Generation << ", send TEvNoSession");
        SendNoSession(ev->Sender, ev->Cookie);
        return nullptr;
    }
    if (!session.EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        SRC_LOG_W("Ignore " << typeid(TEventPtr).name() << " from " << ev->Sender << ", wrong seq num, seqNo " << meta.GetSeqNo());
        return nullptr;
    }

    auto expectedStatus = TSession::ESessionStatus::STARTED;
    if constexpr (std::is_same_v<TEventPtr, NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr>) {
        expectedStatus = TSession::ESessionStatus::WAIT_START_SESSION_ACK;
    }

    if (session.Status != expectedStatus) {
        SRC_LOG_E("Wrong " << typeid(TEventPtr).name() << " from " << ev->Sender << " session status " << static_cast<ui64>(session.Status) << " expected " << static_cast<ui64>(expectedStatus));
        return nullptr;
    }
    return &session;
}

void TDqPqRdReadActor::SendNoSession(const NActors::TActorId& recipient, ui64 cookie) {
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvNoSession>();
    Send(recipient, event.release(), 0, cookie);
}

void TDqPqRdReadActor::NotifyCA() {
    Y_DEBUG_ABORT_UNLESS(Parent == this); // called on Parent
    Metrics.InFlyAsyncInputData->Set(1);
    InFlyAsyncInputData = true;
    Counters.NotifyCA++;
    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
}

void TDqPqRdReadActor::UpdateSessions() {
    SRC_LOG_I("UpdateSessions, Sessions size " << Sessions.size());

    if (LastUsedPartitionDistribution != LastReceivedPartitionDistribution) {
        SRC_LOG_I("Distribution is changed, remove sessions");
        for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
            StopSession(sessionInfo);
            ReadActorByEventQueueId.erase(sessionInfo.EventQueueId);
        }
        Sessions.clear();
    }

    for (const auto& [rowDispatcherActorId, partitions] : LastReceivedPartitionDistribution) {
        if (Sessions.contains(rowDispatcherActorId)) {
            continue;
        }

        auto queueId = ++NextEventQueueId;
        Sessions.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(rowDispatcherActorId),
            std::forward_as_tuple(TxId, SelfId(), rowDispatcherActorId, queueId, ++NextGeneration));
        auto& session = Sessions.at(rowDispatcherActorId);
        SRC_LOG_I("Create session to " << rowDispatcherActorId << ", generation " << session.Generation);
        session.Partitions.insert(partitions.begin(), partitions.end());
        ReadActorByEventQueueId[queueId] = rowDispatcherActorId;
    }
    LastUsedPartitionDistribution = LastReceivedPartitionDistribution;
}

void TDqPqRdReadActor::UpdateQueuedSize() {
    ui64 queuedBytes = 0;
    ui64 queuedRows = 0;
    for (auto& [_, sessionInfo] : Sessions) {
        queuedBytes += sessionInfo.QueuedBytes;
        queuedRows += sessionInfo.QueuedRows;
    }
    Parent->IngressStats.QueuedBytes = queuedBytes;
    Parent->IngressStats.QueuedRows = queuedRows;
}

void TDqPqRdReadActor::StartClusterDiscovery() {
    if (StaticDiscovery) {
        if (SourceParams.FederatedClustersSize()) {
            for (auto& federatedCluster : SourceParams.GetFederatedClusters()) {
                auto& cluster = Clusters.emplace_back(
                        NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo {
                            .Name = federatedCluster.GetName(),
                            .Endpoint = federatedCluster.GetEndpoint(),
                            .Path = federatedCluster.GetDatabase(),
                        },
                        federatedCluster.GetPartitionsCount()
                    );
                if (cluster.PartitionsCount == 0) {
                    cluster.PartitionsCount = ReadParams.front().GetPartitioningParams().GetTopicPartitionsCount();
                    SRC_LOG_W("PartitionsCount for offline server assumed to be " << cluster.PartitionsCount);
                }
            }
        } else { // old AST fallback
            Clusters.emplace_back(
                NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo {
                    .Endpoint = SourceParams.GetEndpoint(),
                    .Path = SourceParams.GetDatabase(),
                },
                ReadParams.front().GetPartitioningParams().GetTopicPartitionsCount()
            );
        }
        for (ui32 clusterIndex = 0; clusterIndex < Clusters.size(); ++clusterIndex) {
            StartCluster(clusterIndex);
        }
        return;
    }
    GetFederatedTopicClient()
        .GetAllTopicClusters()
        .Subscribe([
            actorSystem = NActors::TActivationContext::ActorSystem(),
            selfId = SelfId()](const auto& future)
            {
                try {
                    auto federatedClusters = future.GetValue();
                    actorSystem->Send(selfId, new TEvPrivate::TEvReceivedClusters(std::move(federatedClusters)));
                } catch (const std::exception& ex) {
                    actorSystem->Send(selfId, new TEvPrivate::TEvReceivedClusters(ex));
                }
            });
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvReceivedClusters::TPtr& ev) {
    SRC_LOG_D("Got cluster info");
    auto& federatedClusters = ev->Get()->FederatedClusters;
    if (federatedClusters.empty()) {
        TStringBuilder message;
        message << "Failed to discover clusters for topic \"" << SourceParams.GetTopicPath() << "\"";
        if (ev->Get()->ExceptionMessage) {
            message << ", got exception: " << *ev->Get()->ExceptionMessage;
        } else {
            message << ", empty clusters list";
        }
        SRC_LOG_E(message);
        TIssue issue(message);
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
        return;
    }
    Y_ENSURE(!federatedClusters.empty());
    Clusters.reserve(federatedClusters.size());
    ui32 index = 0;
    for (auto& cluster : federatedClusters) {
        auto& clusterState = Clusters.emplace_back(std::move(cluster), 0);
        SRC_LOG_D(index << " Name " << clusterState.Info.Name << " Endpoint " << clusterState.Info.Endpoint << " Path " << clusterState.Info.Path << " Status " << (int)clusterState.Info.Status);
        std::string clusterTopicPath = SourceParams.GetTopicPath();
        clusterState.Info.AdjustTopicPath(clusterTopicPath);
        GetTopicClient(clusterState)
            .DescribeTopic(TString(clusterTopicPath), {})
            .Subscribe([
                index,
                actorSystem = NActors::TActivationContext::ActorSystem(),
                selfId = SelfId()](const auto& describeTopicFuture)
            {
                try {
                    auto& describeTopic = describeTopicFuture.GetValue();
                    if (!describeTopic.IsSuccess()) {
                        actorSystem->Send(selfId, new TEvPrivate::TEvDescribeTopicResult(index, describeTopic));
                        return;
                    }
                    auto partitionsCount = describeTopic.GetTopicDescription().GetTotalPartitionsCount();
                    actorSystem->Send(selfId, new TEvPrivate::TEvDescribeTopicResult(index, partitionsCount));
                } catch (const std::exception& ex) {
                    actorSystem->Send(selfId, new TEvPrivate::TEvDescribeTopicResult(index,
                                NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR,
                                    NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(ex.what())}))));
                    return;
                }
            });
        index++;
    }
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvDescribeTopicResult::TPtr& ev) {
    auto clusterIndex = ev->Get()->ClusterIndex;
    auto partitionsCount = ev->Get()->PartitionsCount;
    if (auto status = ev->Get()->Status) {
        TStringBuilder message;
        message << "Failed to describe topic \"" << SourceParams.GetTopicPath() << "\"";
        if (!Clusters[clusterIndex].Info.Name.empty()) {
           message << " on cluster \"" << Clusters[clusterIndex].Info.Name << "\"";
        }
        SRC_LOG_E(message);
        TIssue issue(message);
        for (auto& subIssue : status->GetIssues()) {
            TIssuePtr newIssue(new TIssue(NYdb::NAdapters::ToYqlIssue(subIssue)));
            issue.AddSubIssue(newIssue);
        }
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
        return;
    }
    SRC_LOG_D("Got partition info for cluster " << clusterIndex << ", partition count " << partitionsCount);
    Y_ENSURE(Clusters[clusterIndex].PartitionsCount == 0); // TODO Handle refresh
    Y_ENSURE(partitionsCount >= Clusters[clusterIndex].PartitionsCount);
    Clusters[clusterIndex].PartitionsCount = partitionsCount;
    StartCluster(clusterIndex);
}

void TDqPqRdReadActor::StartCluster(ui32 clusterIndex) {
    if (Clusters.size() == 1 && Clusters[clusterIndex].Info.Name.empty()) {
        SRC_LOG_D("Switch to single-cluster mode");
        FederatedTopicClient.Reset();
        Clusters[clusterIndex].Child = this;
        Clusters[clusterIndex].ChildId = SelfId();
        SourceParams.SetEndpoint(TString(Clusters[clusterIndex].Info.Endpoint));
        SourceParams.SetDatabase(TString(Clusters[clusterIndex].Info.Path));
        ReadParams.front().mutable_partitioningparams()->SetTopicPartitionsCount(Clusters[clusterIndex].PartitionsCount);
        State = EState::INIT;
        Init();
        InitChild();
        ProcessState();
        return;
    }
    NPq::NProto::TDqPqTopicSource sourceParams = SourceParams;
    sourceParams.SetEndpoint(TString(Clusters[clusterIndex].Info.Endpoint));
    sourceParams.SetDatabase(TString(Clusters[clusterIndex].Info.Path));
    TVector<NPq::NProto::TDqReadTaskParams> readParams = ReadParams;
    auto actor = new TDqPqRdReadActor(
        InputIndex,
        IngressStats.Level,
        TxId,
        TaskId,
        HolderFactory,
        TypeEnv,
        std::move(sourceParams),
        std::move(readParams),
        Driver,
        CredentialsProviderFactory,
        ComputeActorId,
        LocalRowDispatcherActorId,
        Token,
        {},
        MaxBufferSize,
        PqGateway,
        EnableStreamingQueriesCounters,
        this,
        TString(Clusters[clusterIndex].Info.Name));
    Clusters[clusterIndex].Child = actor;
    Clusters[clusterIndex].ChildId = RegisterWithSameMailbox(actor);
    actor->Init();
    actor->InitChild();
    ProcessState();
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvRefreshClusters::TPtr&) {
    Y_ENSURE(false); // TBD
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvNotifyCA::TPtr&) {
    Schedule(TDuration::Seconds(NotifyCAPeriodSec), new TEvPrivate::TEvNotifyCA());
    NotifyCA();
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvPartitionIdleness::TPtr& ev) {
    if (WatermarkTracker->ProcessIdlenessCheck(ev->Get()->NotifyTime)) {
        NotifyCA();
    }
}

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
    const TTypeEnvironment& typeEnv,
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    TVector<NPq::NProto::TDqReadTaskParams>&& readTaskParamsMsg,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NActors::TActorId& computeActorId,
    const NActors::TActorId& localRowDispatcherActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    i64 bufferSize,
    const IPqGateway::TPtr& pqGateway,
    bool enableStreamingQueriesCounters)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqRdReadActor* actor = new TDqPqRdReadActor(
        inputIndex,
        statsLevel,
        txId,
        taskId,
        holderFactory,
        typeEnv,
        std::move(settings),
        std::move(readTaskParamsMsg),
        driver,
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        computeActorId,
        localRowDispatcherActorId,
        token,
        counters,
        bufferSize,
        pqGateway,
        enableStreamingQueriesCounters);

    return {actor, actor};
}

} // namespace NYql::NDq
