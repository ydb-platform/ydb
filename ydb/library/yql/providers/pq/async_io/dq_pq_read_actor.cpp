#include "dq_pq_read_actor.h"
#include "dq_pq_meta_extractor.h"
#include "dq_pq_rd_read_actor.h"
#include "dq_pq_read_actor_base.h"
#include "probes.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/providers/pq/common/pq_events_processor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/pq_partition_key.h>
#include <ydb/library/yql/providers/pq/gateway/clients/composite/yql_pq_composite_read_session.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/join.h>

#include <queue>
#include <variant>

#define SRC_LOG_T(s) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) \
    LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) \
    LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) \
    LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) \
    LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG(prio, s) \
    LOG_LOG_S(*TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvSourceDataReady = EvBegin,
        EvReconnectSession,
        EvReceivedClusters,
        EvDescribeTopicResult,
        EvExecuteTopicEvent,
        EvPartitionIdleness,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events

    struct TEvSourceDataReady : public TEventLocal<TEvSourceDataReady, EvSourceDataReady> {};

    struct TEvPartitionIdleness : public TEventLocal<TEvPartitionIdleness, EvPartitionIdleness> {
        explicit TEvPartitionIdleness(TInstant notifyTime)
            : NotifyTime(notifyTime)
        {}

        TInstant NotifyTime;
    };

    struct TEvReconnectSession : public TEventLocal<TEvReconnectSession, EvReconnectSession> {};

    struct TEvReceivedClusters : public TEventLocal<TEvReceivedClusters, EvReceivedClusters> {
        explicit TEvReceivedClusters(std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>&& federatedClusters)
            : FederatedClusters(std::move(federatedClusters))
        {}

        explicit TEvReceivedClusters(const std::exception& ex)
            : ExceptionMessage(ex.what())
        {}

        std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> FederatedClusters;
        std::optional<std::string> ExceptionMessage;
    };

    struct TEvDescribeTopicResult : public TEventLocal<TEvDescribeTopicResult, EvDescribeTopicResult> {
        TEvDescribeTopicResult(ui32 clusterIndex, ui32 partitionsCount)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(partitionsCount)
        {}

        TEvDescribeTopicResult(ui32 clusterIndex, const NYdb::TStatus& status)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(0)
            , Status(status)
        {}

        const ui32 ClusterIndex = 0;
        const ui32 PartitionsCount = 0;
        TMaybe<NYdb::TStatus> Status;
    };

    struct TEvExecuteTopicEvent : public TTopicEventBase<TEvExecuteTopicEvent, EvExecuteTopicEvent> {
        using TTopicEventBase::TTopicEventBase;
    };
};

} // anonymous namespace

class TDqPqReadActor : public TActor<TDqPqReadActor>, public NYql::NDq::NInternal::TDqPqReadActorBase, TTopicEventProcessor<TEvPrivate::TEvExecuteTopicEvent> {
    static constexpr bool STATIC_DISCOVERY = true;
    static constexpr TDuration CHECK_HANGING_PERIOD = TDuration::Minutes(1);

    struct TMetrics {
        TMetrics(const TTxId& txId, ui64 taskId, const ::NMonitoring::TDynamicCounterPtr& counters,
            const NPq::NProto::TDqPqTopicSource& sourceParams, bool enableStreamingQueriesCounters)
            : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
            , Counters(counters)
        {
            if (counters) {
                SubGroup = Counters->GetSubgroup("source", "PqRead");
            } else {
                SubGroup = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            }

            Source = SubGroup;
            Task = SubGroup;
            if (enableStreamingQueriesCounters) {
                for (const auto& sensor : sourceParams.GetTaskSensorLabel()) {
                    SubGroup = SubGroup->GetSubgroup(sensor.GetLabel(), sensor.GetValue());
                }
                Source = SubGroup->GetSubgroup("tx_id", TxId);
                Task = Source->GetSubgroup("task_id", ToString(taskId));
            }
            InFlyAsyncInputData = Task->GetCounter("InFlyAsyncInputData");
            InFlySubscribe = Task->GetCounter("InFlySubscribe");
            AsyncInputDataRate = Task->GetCounter("AsyncInputDataRate", true);
            ReconnectRate = Task->GetCounter("ReconnectRate", true);
            DataRate = Task->GetCounter("DataRate", true);
            WaitEventTimeMs = Source->GetHistogram("WaitEventTimeMs", NMonitoring::ExplicitHistogram({5, 20, 100, 500, 2000}));
        }

        ~TMetrics() {
            if (SubGroup) {
                SubGroup->RemoveSubgroup("tx_id", TxId);
            }
        }

        TString TxId;
        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounterPtr SubGroup;
        ::NMonitoring::TDynamicCounterPtr Task;
        ::NMonitoring::TDynamicCounterPtr Source;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
        ::NMonitoring::TDynamicCounters::TCounterPtr AsyncInputDataRate;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReconnectRate;
        ::NMonitoring::TDynamicCounters::TCounterPtr DataRate;
        NMonitoring::THistogramPtr WaitEventTimeMs;
    };

    struct TClusterState {
        TClusterState(ui32 index, NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo&& info, ui32 partitionsCount)
            : Index(index)
            , Info(std::move(info))
            , PartitionsCount(partitionsCount)
        {}

        ui32 Index = 0;
        NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo Info;
        ITopicClient::TPtr TopicClient;
        std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
        ICompositeTopicReadSessionControl::TPtr ReadSessionControl;
        ui32 PartitionsCount = 0;
        NThreading::TFuture<void> EventFuture;
        bool SubscribedOnEvent = false;
        TMaybe<TInstant> WaitEventStartedAt;
    };

public:
    using TPartitionKey = ::NPq::TPartitionKey;
    using TDebugOffsets = TMaybe<std::pair<ui64, ui64>>;

    TDqPqReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        std::shared_ptr<TScopedAlloc> alloc,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const TActorId& computeActorId,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize,
        const IPqGateway::TPtr& pqGateway,
        ui32 topicPartitionsCount,
        bool enableStreamingQueriesCounters,
        TActorId infoAggregator)
        : TActor<TDqPqReadActor>(&TDqPqReadActor::StateFunc)
        , TDqPqReadActorBase(inputIndex, taskId, this->SelfId(), txId, std::move(sourceParams), std::move(readParams), computeActorId)
        , Metrics(txId, taskId, counters, SourceParams, enableStreamingQueriesCounters)
        , BufferSize(bufferSize)
        , HolderFactory(holderFactory)
        , Alloc(std::move(alloc))
        , InfoAggregator(infoAggregator)
        , Driver(std::move(driver))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , PqGateway(pqGateway)
        , TopicPartitionsCount(topicPartitionsCount)
        , WithoutConsumer(SourceParams.GetConsumerName().empty())
    {
        if (const auto& period = SourceParams.GetReconnectPeriod(); !TDuration::TryParse(period, ReconnectPeriod)) {
            SRC_LOG_N("Failed to parse reconnect period: " << period);
        }

        MetadataFields.reserve(SourceParams.MetadataFieldsSize());
        TPqMetaExtractor fieldsExtractor;
        for (const auto& fieldName : SourceParams.GetMetadataFields()) {
            MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
        }

        InitWatermarkTracker(); // non-virtual!
        IngressStats.Level = statsLevel;
    }

    ~TDqPqReadActor() {
        if (Alloc) {
            TGuard<TScopedAlloc> allocGuard(*Alloc);
            ClearMkqlData();
        }
    }

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const {
        NYdb::NFederatedTopic::TFederatedTopicClientSettings opts = PqGateway->GetFederatedTopicClientSettings();
        opts.Database(SourceParams.GetDatabase())
            .DiscoveryEndpoint(SourceParams.GetEndpoint())
            .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsProviderFactory);

        return opts;
    }

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings(TClusterState& state) {
        NYdb::NTopic::TTopicClientSettings opts = PqGateway->GetTopicClientSettings();

        if (SourceParams.GetUseActorSystemThreadsInTopicClient()) {
            SetupTopicClientSettings(ActorContext().ActorSystem(), SelfId(), opts);
        }

        opts.Database(SourceParams.GetDatabase())
            .DiscoveryEndpoint(SourceParams.GetEndpoint())
            .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsProviderFactory);
        state.Info.AdjustTopicClientSettings(opts);

        return opts;
    }

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

public:
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
        TDqPqReadActorBase::SaveState(checkpoint, state);
        if (!WithoutConsumer) {
            DeferredCommits.emplace(checkpoint.GetId(), std::move(CurrentDeferredCommit));
            CurrentDeferredCommit = NYdb::NTopic::TDeferredCommit();
        }
    }

    void LoadState(const TSourceState& state) override {
        TDqPqReadActorBase::LoadState(state);

        Clusters.clear();
        AsyncInit = {};
        StartClusterDiscovery();
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        const auto checkpointId = checkpoint.GetId();
        if (!WithoutConsumer) {
            while (!DeferredCommits.empty() && DeferredCommits.front().first <= checkpointId) {
                auto& deferredCommit = DeferredCommits.front().second;
                deferredCommit.Commit();
                DeferredCommits.pop();
            }
        }
    }

    IFederatedTopicClient& GetFederatedTopicClient() {
        if (!FederatedTopicClient) {
            FederatedTopicClient = PqGateway->GetFederatedTopicClient(Driver, GetFederatedTopicClientSettings());
        }
        return *FederatedTopicClient;
    }

    ITopicClient& GetTopicClient(TClusterState& clusterState) {
        if (!clusterState.TopicClient) {
            clusterState.TopicClient = PqGateway->GetTopicClient(Driver, GetTopicClientSettings(clusterState));
        }
        return *clusterState.TopicClient;
    }

    NYdb::NTopic::IReadSession& GetReadSession(TClusterState& clusterState) {
        if (!clusterState.ReadSession) {
            if (const auto maxPartitionReadSkew = NProtoInterop::CastFromProto(SourceParams.GetMaxPartitionReadSkew())) {
                YQL_ENSURE(InfoAggregator, "Missing DQ info aggregator for distributed read session");

                ui64 amountPartitions = 0;
                for (const auto& cluster : Clusters) {
                    amountPartitions += cluster.PartitionsCount;
                }

                auto counters = Metrics.Task;
                if (!clusterState.Info.Name.empty()) {
                    counters = counters->GetSubgroup("federated_pq_cluster", TString(clusterState.Info.Name));
                }

                std::tie(clusterState.ReadSession, clusterState.ReadSessionControl) = CreateCompositeTopicReadSession(ActorContext(), GetTopicClient(clusterState), {
                    .TxId = TxId,
                    .TaskId = TaskId,
                    .Cluster = TString(clusterState.Info.Name),
                    .AmountPartitionsCount = amountPartitions,
                    .InputIndex = InputIndex,
                    .Counters = counters,
                    .BaseSettings = GetReadSessionSettings(clusterState),
                    .IdleTimeout = NProtoInterop::CastFromProto(SourceParams.GetPartitionsBalancingIdleTimeout()),
                    .MaxPartitionReadSkew = maxPartitionReadSkew,
                    .AggregatorActor = InfoAggregator,
                });
            } else {
                clusterState.ReadSession = GetTopicClient(clusterState).CreateReadSession(GetReadSessionSettings(clusterState));
            }

            SRC_LOG_I("SessionId: " << GetSessionId(clusterState.Index) << " CreateReadSession");
            if (WatermarkTracker) {
                TPartitionKey partitionKey { .Cluster = TString(clusterState.Info.Name) };
                auto now = TInstant::Now();
                for (const auto partitionId : GetPartitionsToRead(clusterState)) { // XXX duplicated, but rare
                    partitionKey.PartitionId = partitionId;
                    WatermarkTracker->RegisterPartition(partitionKey, now);
                }
            }
        }
        return *clusterState.ReadSession;
    }

    TString GetSessionId() const override {
        if (Clusters.empty()) {
            return "empty";
        }

        TStringBuilder str;
        for (const auto& clusterState : Clusters) {
            if (auto readSession = clusterState.ReadSession) {
                str << readSession->GetSessionId();
            } else {
                str << "empty";
            }
            str << ',';
        }

        str.pop_back();
        return str;
    }

    TString GetSessionId(ui32 index) const {
        return !Clusters.empty() && Clusters[index].ReadSession ? TString{Clusters[index].ReadSession->GetSessionId()} : TString{"empty"};
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSourceDataReady, Handle);
        hFunc(TEvPrivate::TEvPartitionIdleness, Handle);
        hFunc(TEvPrivate::TEvReconnectSession, Handle);
        hFunc(TEvPrivate::TEvReceivedClusters, Handle);
        hFunc(TEvPrivate::TEvDescribeTopicResult, Handle);
        hFunc(TEvPrivate::TEvExecuteTopicEvent, HandleTopicEvent);
        hFunc(TEvents::TEvWakeup, Handle);
    )

    void Handle(TEvPrivate::TEvSourceDataReady::TPtr& ev) {
        if (ev.Get()->Cookie && !Clusters.empty()) {
            auto index = ev.Get()->Cookie - 1;
            auto& clusterState = Clusters[index];
            SRC_LOG_T("SessionId: " << GetSessionId(index) << " Source data ready");
            clusterState.SubscribedOnEvent = false;
            Metrics.InFlySubscribe->Dec();
            if (clusterState.WaitEventStartedAt) {
                auto waitEventDurationMs = (TInstant::Now() - *clusterState.WaitEventStartedAt).MilliSeconds();
                Metrics.WaitEventTimeMs->Collect(waitEventDurationMs);
                clusterState.WaitEventStartedAt.Clear();
            }
        }
        NotifyCA();
    }

    void NotifyCA() {
        Metrics.InFlyAsyncInputData->Set(1);
        Metrics.AsyncInputDataRate->Inc();
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvPrivate::TEvPartitionIdleness::TPtr& ev) {
        if (WatermarkTracker->ProcessIdlenessCheck(ev->Get()->NotifyTime)) {
            NotifyCA();
        }
    }

    void Handle(TEvPrivate::TEvReconnectSession::TPtr&) {
        for (auto& clusterState : Clusters) {
            SRC_LOG_D("SessionId: " << GetSessionId(clusterState.Index) << ", Reconnect epoch: " << (Metrics.ReconnectRate ? Metrics.ReconnectRate->Val() : 0));
            if (clusterState.ReadSession) {
                clusterState.ReadSession->Close(TDuration::Zero());
                clusterState.ReadSession.reset();
            }
        }
        Reconnected = true;
        Metrics.ReconnectRate->Inc();

        Schedule(ReconnectPeriod, new TEvPrivate::TEvReconnectSession());
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        ClearMkqlData();

        for (auto& clusterState : Clusters) {
            if (clusterState.ReadSession) {
                clusterState.ReadSession->Close(TDuration::Zero());
                clusterState.ReadSession.reset();
            }
            clusterState.TopicClient.Reset();
        }
        FederatedTopicClient.Reset();
        TActor<TDqPqReadActor>::PassAway();
    }

    void StartClusterDiscovery() {
        Y_ENSURE(Clusters.empty());

        if (STATIC_DISCOVERY) {
            ui32 index = 0;
            if (SourceParams.FederatedClustersSize()) {
                for (const auto& federatedCluster : SourceParams.GetFederatedClusters()) {
                    auto& cluster = Clusters.emplace_back(
                        index++,
                        NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo {
                            .Name = federatedCluster.GetName(),
                            .Endpoint = federatedCluster.GetEndpoint(),
                            .Path = federatedCluster.GetDatabase(),
                        },
                        TopicPartitionsCount
                    );
                    if (cluster.PartitionsCount == 0) {
                        cluster.PartitionsCount = TopicPartitionsCount;
                        SRC_LOG_W("PartitionsCount for offline server assumed to be " << cluster.PartitionsCount);
                    }
                }
            } else {
                Clusters.emplace_back(
                    index++,
                    NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo {
                        .Endpoint = SourceParams.GetEndpoint(),
                        .Path =SourceParams.GetDatabase()
                    },
                    TopicPartitionsCount
                );
            }

            Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
            return;
        }

        if (AsyncInit.Initialized()) {
            return;
        }

        AsyncInit = GetFederatedTopicClient().GetAllTopicClusters();
        AsyncInit.Subscribe([
            actorSystem = TActivationContext::ActorSystem(),
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

    void Handle(TEvPrivate::TEvReceivedClusters::TPtr& ev) {
        // TODO support refresh
        SRC_LOG_D("Got cluster info");
        auto& federatedClusters = ev->Get()->FederatedClusters;
        if (federatedClusters.empty()) {
            TStringBuilder message;
            message << "Failed to get clusters topic \"" << SourceParams.GetTopicPath() << "\"";
            if (ev->Get()->ExceptionMessage) {
                message << ", got exception: " << *ev->Get()->ExceptionMessage;
            } else {
                message << ", empty clusters list";
            }
            TIssue issue(message);
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
            return;
        }

        Clusters.reserve(federatedClusters.size());
        ui32 index = 0;
        for (auto& cluster : federatedClusters) {
            auto& clusterState = Clusters.emplace_back(index, std::move(cluster), 0u);
            SRC_LOG_D(index << " Name " << clusterState.Info.Name << " Endpoint " << clusterState.Info.Endpoint << " Path " << clusterState.Info.Path << " Status " << (int)clusterState.Info.Status);
            std::string clusterTopicPath = SourceParams.GetTopicPath();
            clusterState.Info.AdjustTopicPath(clusterTopicPath);

            GetTopicClient(clusterState)
                .DescribeTopic(TString(clusterTopicPath), {})
                .Subscribe([
                    index,
                    actorSystem = TActivationContext::ActorSystem(),
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
                            NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, {NYdb::NIssue::TIssue(ex.what())})
                        ));
                        return;
                    }
                });

            index++;
        }
    }

    void Handle(TEvPrivate::TEvDescribeTopicResult::TPtr& ev) {
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

        SRC_LOG_D("Got partition info for cluster " << clusterIndex << " = " << partitionsCount);
        Clusters[clusterIndex].PartitionsCount = partitionsCount;
        Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        WakeupScheduled = false;
        ScheduleWakeup();

        if (TInstant::Now() - LastActiveTime <= CHECK_HANGING_PERIOD) {
            return;
        }
        LastActiveTime = TInstant::Now();

        for (const auto& clusterState : Clusters) {
            if (const auto& readSession = clusterState.ReadSession) {
                SRC_LOG_D("Check Read Session, cluster: " << clusterState.Info.Name << ", is ready: " << readSession->WaitEvent().IsReady());
            }
        }
    }

    void ScheduleWakeup() {
        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(CHECK_HANGING_PERIOD, new TEvents::TEvWakeup());
        }
    }

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override {
        // called with bound allocator
        Metrics.InFlyAsyncInputData->Set(0);
        SRC_LOG_T("SessionId: " << GetSessionId() << " GetAsyncInputData freeSpace = " << freeSpace);

        const auto now = TInstant::Now();

        if (!InflightReconnect && ReconnectPeriod != TDuration::Zero()) {
            Metrics.ReconnectRate->Inc();
            Schedule(ReconnectPeriod, new TEvPrivate::TEvReconnectSession());
            InflightReconnect = true;
        }

        if (Reconnected) {
            Reconnected = false;
            ReadyBuffer = std::queue<TReadyBatch>{}; // clear read buffer
        }

        i64 usedSpace = 0;
        if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
            return usedSpace;
        }

        bool recheckBatch = false;
        if (freeSpace > 0) {
            if (Clusters.empty()) {
                StartClusterDiscovery();
            }
            for (auto& clusterState : Clusters) {
                if (clusterState.PartitionsCount == 0) {
                    continue;
                }
                auto events = GetReadSession(clusterState).GetEvents(false, std::nullopt, static_cast<size_t>(freeSpace));
                if (!events.empty()) {
                    recheckBatch = true;
                }

                ui32 batchItemsEstimatedCount = 0;
                for (auto& event : events) {
                    if (const auto* val = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        batchItemsEstimatedCount += val->GetMessages().size();
                    }
                }

                TTopicEventProcessor topicEventProcessor {*this, clusterState, batchItemsEstimatedCount, LogPrefix, TString(clusterState.Info.Name), clusterState.Index };
                for (auto& event : events) {
                    std::visit(topicEventProcessor, event);
                }
            }
        }

        if (WatermarkTracker) {
            WatermarkTracker->ProcessIdlenessCheck(now); // drop obsolete checks
            const auto watermark = WatermarkTracker->HandleIdleness(now);

            if (watermark) {
                SRC_LOG_T("SessionId: " << GetSessionId() << " Idleness watermark " << *watermark << " was produced");
                PushWatermarkToReady(*watermark);
                recheckBatch = true;
            }
            MaybeSchedulePartitionIdlenessCheck(now);
        }

        if (recheckBatch) {
            LastActiveTime = TInstant::Now();
            ScheduleWakeup();

            usedSpace = 0;
            if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
                return usedSpace;
            }
        }

        watermark = Nothing();
        buffer.clear();
        return 0;
    }

    std::vector<ui64> GetPartitionsToRead(TClusterState& clusterState) const {
        std::vector<ui64> res;

        for (const auto& readParams : ReadParams) {
            ui64 currentPartition = readParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
            while (currentPartition < clusterState.PartitionsCount) {
                res.emplace_back(currentPartition); // 0-based in topic API
                currentPartition += readParams.GetPartitioningParams().GetDqPartitionsCount();
            }
        }

        return res;
    }

    void InitWatermarkTracker() override {
        auto lateArrivalDelayUs = SourceParams.GetWatermarks().GetLateArrivalDelayUs();
        auto idleTimeoutUs = // TODO remove fallback
            SourceParams.GetWatermarks().HasIdleTimeoutUs() ?
            SourceParams.GetWatermarks().GetIdleTimeoutUs() :
            lateArrivalDelayUs;
        TDqPqReadActorBase::InitWatermarkTracker(TDuration::MicroSeconds(lateArrivalDelayUs), TDuration::MicroSeconds(idleTimeoutUs), Metrics.Counters ? Metrics.Source : nullptr);
    }

    void SchedulePartitionIdlenessCheck(TInstant at) override {
        Schedule(at, new TEvPrivate::TEvPartitionIdleness(at));
    }

    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings(TClusterState& clusterState) const {
        NYdb::NTopic::TTopicReadSettings topicReadSettings;
        std::string topicPath = SourceParams.GetTopicPath();
        clusterState.Info.AdjustTopicPath(topicPath);
        topicReadSettings.Path(topicPath);

        const auto& partitionsToRead = GetPartitionsToRead(clusterState);
        SRC_LOG_D("SessionId: " << GetSessionId(clusterState.Index) << " PartitionsToRead: " << JoinSeq(", ", partitionsToRead));
        for (const auto partitionId : partitionsToRead) {
            topicReadSettings.AppendPartitionIds(partitionId);
        }

        auto settings = NYdb::NTopic::TReadSessionSettings();
        settings
            .AppendTopics(topicReadSettings)
            .MaxMemoryUsageBytes(BufferSize)
            .ReadFromTimestamp(StartingMessageTimestamp);

        if (!WithoutConsumer) {
            settings.ConsumerName(SourceParams.GetConsumerName());
        } else {
            settings.WithoutConsumer();
        }

        return settings;
    }

    static TPartitionKey MakePartitionKey(const TString& cluster, const NYdb::NTopic::TPartitionSession::TPtr& partitionSession) {
        return { cluster, partitionSession->GetPartitionId() };
    }

    void SubscribeOnNextEvent() {
        for (auto& clusterState : Clusters) {
            SubscribeOnNextEvent(clusterState);
        }
    }

    void SubscribeOnNextEvent(TClusterState& clusterState) {
        if (!clusterState.SubscribedOnEvent) {
            clusterState.SubscribedOnEvent = true;
            Metrics.InFlySubscribe->Inc();
            TActorSystem* actorSystem = TActivationContext::ActorSystem();
            clusterState.WaitEventStartedAt = TInstant::Now();
            clusterState.EventFuture = GetReadSession(clusterState).WaitEvent().Subscribe([actorSystem, selfId = SelfId(), index = clusterState.Index](const auto&){
                actorSystem->Send(selfId, new TEvPrivate::TEvSourceDataReady(), 0, 1 + index);
            });
        }
    }

    struct TReadyBatch {
    public:
        TReadyBatch(TMaybe<TInstant> watermark, ui32 dataCapacity)
          : Watermark(watermark) {
            Data.reserve(dataCapacity);
        }

    public:
        TMaybe<TInstant> Watermark;
        TUnboxedValueVector Data;
        i64 UsedSpace = 0;
        THashMap<NYdb::NTopic::TPartitionSession::TPtr, std::pair<std::string, TList<std::pair<ui64, ui64>>>> OffsetRanges; // [start, end)
    };

    // must be called with bound allocator
    bool MaybeReturnReadyBatch(TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, i64& usedSpace) {
        if (ReadyBuffer.empty()) {
            SubscribeOnNextEvent();
            return false;
        }

        auto& readyBatch = ReadyBuffer.front();
        buffer.clear();
        std::move(readyBatch.Data.begin(), readyBatch.Data.end(), std::back_inserter(buffer));
        watermark = readyBatch.Watermark;
        usedSpace = readyBatch.UsedSpace;
        Metrics.DataRate->Add(readyBatch.UsedSpace);

        for (const auto& [partitionSession, clusterRanges] : readyBatch.OffsetRanges) {
            const auto& [cluster, ranges] = clusterRanges;
            if (!WithoutConsumer) {
                for (const auto& [start, end] : ranges) {
                    CurrentDeferredCommit.Add(partitionSession, start, end);
                }
            }
            PartitionToOffset[MakePartitionKey(TString(cluster), partitionSession)] = ranges.back().second;
        }

        ReadyBuffer.pop();

        if (ReadyBuffer.empty()) {
            SubscribeOnNextEvent();
        } else {
            Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
        }

        SRC_LOG_T("SessionId: " << GetSessionId() << " Return ready batch."
            << " DataCount = " << buffer.RowCount()
            << " Watermark = " << (watermark ? ToString(*watermark) : "none")
            << " Used space = " << usedSpace);
        return true;
    }

    // must be called with bound allocator
    void PushWatermarkToReady(TInstant watermark) {
        SRC_LOG_D("SessionId: " << GetSessionId() << " New watermark " << watermark << " was generated");

        if (Y_UNLIKELY(ReadyBuffer.empty() || ReadyBuffer.back().Watermark.Defined())) {
            ReadyBuffer.emplace(watermark, 0);
            return;
        }

        ReadyBuffer.back().Watermark = watermark;
    }

    // must be called with bound allocator
    void ClearMkqlData() {
        std::queue<TReadyBatch> empty;
        ReadyBuffer.swap(empty);
    }

    // must be called (visited) with bound allocator
    struct TTopicEventProcessor {
        void operator()(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.GetPartitionSession());
            for (const auto& message : event.GetMessages()) {
                const std::string& data = message.GetData();
                Self.IngressStats.Bytes += data.size();
                LWPROBE(PqReadDataReceived, TString(TStringBuilder() << Self.TxId), Self.SourceParams.GetTopicPath(), TString{data});
                SRC_LOG_T("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Data received: " << message.DebugString(true));

                if (ClusterState.ReadSessionControl) {
                    ClusterState.ReadSessionControl->AdvancePartitionTime(message.GetPartitionSession()->GetPartitionId(), message.GetWriteTime());
                }

                if (message.GetWriteTime() < Self.StartingMessageTimestamp) {
                    SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Skip data. StartingMessageTimestamp: " << Self.StartingMessageTimestamp << ". Write time: " << message.GetWriteTime());
                    continue;
                }

                auto [item, size] = CreateItem(message);
                const auto partitionTime = message.GetWriteTime();

                if (Self.ReadyBuffer.empty() || Self.ReadyBuffer.back().Watermark.Defined()) {
                    Self.ReadyBuffer.emplace(Nothing(), BatchCapacity);
                }
                TReadyBatch& activeBatch = Self.ReadyBuffer.back();

                activeBatch.Data.emplace_back(std::move(item));
                activeBatch.UsedSpace += size;

                auto& [cluster, offsets] = activeBatch.OffsetRanges[message.GetPartitionSession()];
                cluster = Cluster;

                if (!offsets.empty() && offsets.back().second == message.GetOffset()) {
                    offsets.back().second = message.GetOffset() + 1;
                } else {
                    offsets.emplace_back(message.GetOffset(), message.GetOffset() + 1);
                }

                if (!Self.WatermarkTracker) {
                    continue;
                }
                const auto maybeNewWatermark = Self.WatermarkTracker->NotifyNewPartitionTime(
                    partitionKey,
                    partitionTime,
                    TInstant::Now()
                );
                if (!maybeNewWatermark) {
                    continue;
                }
                activeBatch.Watermark = *maybeNewWatermark;
                SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " New watermark " << *maybeNewWatermark << " was generated");
            }
        }

        void operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
            const auto& LogPrefix = Self.LogPrefix;
            TString message = (TStringBuilder() << "Read session to topic \"" << Self.SourceParams.GetTopicPath() << "\" was closed");
            SRC_LOG_E("SessionId: " << Self.GetSessionId(Index) << " " << message << ": " << ev.DebugString());
            TIssue issue(message);
            for (const auto& subIssue : ev.GetIssues()) {
                TIssuePtr newIssue(new TIssue(NYdb::NAdapters::ToYqlIssue(subIssue)));
                issue.AddSubIssue(newIssue);
            }
            Self.Send(Self.ComputeActorId, new TEvAsyncInputError(Self.InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) { }

        void operator()(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.GetPartitionSession());
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " StartPartitionSessionEvent received");

            std::optional<ui64> readOffset;
            const auto offsetIt = Self.PartitionToOffset.find(partitionKey);
            if (offsetIt != Self.PartitionToOffset.end()) {
                readOffset = offsetIt->second;
            }
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Confirm StartPartitionSession with offset " << readOffset);
            event.Confirm(readOffset);
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.GetPartitionSession());
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " StopPartitionSessionEvent received");
            event.Confirm();
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.GetPartitionSession());
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " EndPartitionSessionEvent received");
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent&) { }

        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.GetPartitionSession());
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " PartitionSessionClosedEvent received");
        }

        std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            const std::string& data = message.GetData();

            i64 usedSpace = 0;
            NUdf::TUnboxedValuePod item;
            if (Self.MetadataFields.empty()) {
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
                usedSpace += data.size();
            } else {
                NUdf::TUnboxedValue* itemPtr;
                item = Self.HolderFactory.CreateDirectArrayHolder(Self.MetadataFields.size() + 1, itemPtr);
                *(itemPtr++) = MakeString(NUdf::TStringRef(data.data(), data.size()));
                usedSpace += data.size();

                for (const auto& [name, extractor] : Self.MetadataFields) {
                    auto [ub, size] = extractor(message);
                    *(itemPtr++) = std::move(ub);
                    usedSpace += size;
                }
            }

            return std::make_pair(item, usedSpace);
        }

        TDqPqReadActor& Self;
        TClusterState& ClusterState;
        ui32 BatchCapacity;
        const TString& LogPrefix;
        const TString& Cluster;
        const ui32 Index;
    };

private:
    bool InflightReconnect = false;
    TDuration ReconnectPeriod;
    bool Reconnected = false;
    TMetrics Metrics;
    const i64 BufferSize;
    const THolderFactory& HolderFactory;
    const std::shared_ptr<TScopedAlloc> Alloc;
    const TActorId InfoAggregator;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    IFederatedTopicClient::TPtr FederatedTopicClient;
    std::vector<TClusterState> Clusters;
    std::queue<std::pair<ui64, NYdb::NTopic::TDeferredCommit>> DeferredCommits;
    NYdb::NTopic::TDeferredCommit CurrentDeferredCommit;
    std::vector<std::tuple<TString, TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    std::queue<TReadyBatch> ReadyBuffer;
    IPqGateway::TPtr PqGateway;
    NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> AsyncInit;
    ui32 TopicPartitionsCount = 0;
    bool WithoutConsumer = false;
    bool WakeupScheduled = false;
    TInstant LastActiveTime = TInstant::Now();
};

ui32 ExtractPartitionsFromParams(
    TVector<NPq::NProto::TDqReadTaskParams>& readTaskParamsMsg,
    const THashMap<TString, TString>& taskParams, // partitions are here in dq
    const TVector<TString>& readRanges            // partitions are here in kqp
) {
    ui32 partitionCount = 0;
    if (!readRanges.empty()) {
        for (const auto& readRange : readRanges) {
            NPq::NProto::TDqReadTaskParams params;
            YQL_ENSURE(params.ParseFromString(readRange), "Failed to parse DqPqRead task params");
            if (!partitionCount) {
                partitionCount = params.GetPartitioningParams().GetTopicPartitionsCount();
            }
            YQL_ENSURE(partitionCount == params.GetPartitioningParams().GetTopicPartitionsCount(),
                "Different partition count " << partitionCount << ", " << params.GetPartitioningParams().GetTopicPartitionsCount());
            readTaskParamsMsg.emplace_back(std::move(params));
        }
    } else {
        auto taskParamsIt = taskParams.find("pq");
        YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");
        NPq::NProto::TDqReadTaskParams params;
        YQL_ENSURE(params.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");
        partitionCount = params.GetPartitioningParams().GetTopicPartitionsCount();
        readTaskParamsMsg.emplace_back(std::move(params));
    }
    return partitionCount;
}

std::pair<IDqComputeActorAsyncInput*, IActor*> CreateDqPqReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    TVector<NPq::NProto::TDqReadTaskParams>&& readTaskParamsMsg,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TActorId& computeActorId,
    const THolderFactory& holderFactory,
    std::shared_ptr<TScopedAlloc> alloc,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    IPqGateway::TPtr pqGateway,
    ui32 topicPartitionsCount,
    bool enableStreamingQueriesCounters,
    i64 bufferSize,
    TActorId infoAggregator
) {
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqReadActor* actor = new TDqPqReadActor(
        inputIndex,
        statsLevel,
        txId,
        taskId,
        holderFactory,
        std::move(alloc),
        std::move(settings),
        std::move(readTaskParamsMsg),
        std::move(driver),
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        computeActorId,
        counters,
        settings.GetReadSessionBufferBytes() ? settings.GetReadSessionBufferBytes() : bufferSize,
        pqGateway,
        topicPartitionsCount,
        enableStreamingQueriesCounters,
        infoAggregator
    );

    return {actor, actor};
}

void RegisterDqPqReadActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, const IPqGateway::TPtr& pqGateway, const ::NMonitoring::TDynamicCounterPtr& counters, const TString& reconnectPeriod, bool enableStreamingQueriesCounters) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>("PqSource",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters, pqGateway, reconnectPeriod, enableStreamingQueriesCounters](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));

        if (reconnectPeriod) {
            settings.SetReconnectPeriod(reconnectPeriod);
        }

        TVector<NPq::NProto::TDqReadTaskParams> readTaskParamsMsg;
        ui32 topicPartitionsCount = ExtractPartitionsFromParams(readTaskParamsMsg, args.TaskParams, args.ReadRanges);

        auto txId = args.TxId;
        auto taskParamsIt = args.TaskParams.find("query_path");
        if (taskParamsIt != args.TaskParams.end()) {
            txId = taskParamsIt->second;
        }

        TActorId infoAggregator;
        if (const auto it = args.TaskParams.find("ControlPlane/PqSourcePartitionBalancerAggregatorId"); it != args.TaskParams.end()) {
            NActorsProto::TActorId actorIdProto;
            YQL_ENSURE(actorIdProto.ParseFromString(it->second), "Failed to parse " << it->first);
            infoAggregator = ActorIdFromProto(actorIdProto);
        }

        if (!settings.GetSharedReading()) {
            return CreateDqPqReadActor(
                std::move(settings),
                args.InputIndex,
                args.StatsLevel,
                txId,
                args.TaskId,
                args.SecureParams,
                std::move(readTaskParamsMsg),
                driver,
                credentialsFactory,
                args.ComputeActorId,
                args.HolderFactory,
                std::move(args.Alloc),
                counters ? counters : args.TaskCounters,
                pqGateway,
                topicPartitionsCount,
                enableStreamingQueriesCounters,
                PQReadDefaultFreeSpace,
                infoAggregator);
        }

        return CreateDqPqRdReadActor(
            args.TypeEnv,
            std::move(settings),
            args.InputIndex,
            args.StatsLevel,
            txId,
            args.TaskId,
            args.SecureParams,
            std::move(readTaskParamsMsg),
            driver,
            credentialsFactory,
            args.ComputeActorId,
            NFq::RowDispatcherServiceActorId(),
            args.HolderFactory,
            counters ? counters : args.TaskCounters,
            PQReadDefaultFreeSpace,
            pqGateway,
            enableStreamingQueriesCounters);
    });
}

} // namespace NYql::NDq
