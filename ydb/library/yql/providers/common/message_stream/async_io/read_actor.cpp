#include "read_actor.h"
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/runtime/streaming/dq_source_watermark_tracker.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>
#include <util/random/random.h>
#include <deque>
#include <set>
#include <map>

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

namespace NFq::NMessageStream {

using namespace NYql;
using namespace NYql::NDq;

using namespace NActors;
using namespace NYql::NLog;
using namespace NKikimr::NMiniKQL;

namespace {



struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvSourceDataReady = EvBegin,
        EvReconnectSession,
        EvExecuteTopicEvent,
        EvPartitionIdleness,
        EvCheckPartitionTimer,
        EvCheckPartitionCount,
        EvCheckPartitionCountResult,
        EvRequestPartitionStatus,
        EvResumeCallbacks,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvResumeCallbacks : public TEventLocal<TEvResumeCallbacks, EvResumeCallbacks> {
        explicit TEvResumeCallbacks(ui64 generation)
            : Generation(generation)
        {}
        const ui64 Generation;
    };

    // Events

    struct TEvSourceDataReady : public TEventLocal<TEvSourceDataReady, EvSourceDataReady> {};

    struct TEvPartitionIdleness : public TEventLocal<TEvPartitionIdleness, EvPartitionIdleness> {
        explicit TEvPartitionIdleness(TInstant notifyTime)
            : NotifyTime(notifyTime)
        {}

        TInstant NotifyTime;
    };

    struct TEvReconnectSession : public TEventLocal<TEvReconnectSession, EvReconnectSession> {};


    struct TEvCheckPartitionTimer : public TEventLocal<TEvCheckPartitionTimer, EvCheckPartitionTimer> {};

    struct TEvRequestPartitionStatus : public TEventLocal<TEvRequestPartitionStatus, EvRequestPartitionStatus> {};

    struct TEvCheckPartitionCount : public TEventLocal<TEvCheckPartitionCount, EvCheckPartitionCount> {
        explicit TEvCheckPartitionCount(ui32 clusterIndex)
            : ClusterIndex(clusterIndex)
        {}
        const ui32 ClusterIndex = 0;
    };

    struct TEvCheckPartitionCountResult : public TEventLocal<TEvCheckPartitionCountResult, EvCheckPartitionCountResult> {
        TEvCheckPartitionCountResult(ui32 clusterIndex, ui32 partitionsCount)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(partitionsCount)
        {}
        TEvCheckPartitionCountResult(ui32 clusterIndex, TIssues issues)
            : ClusterIndex(clusterIndex)
            , PartitionsCount(0)
            , Issues(std::move(issues))
        {}
        const ui32 ClusterIndex = 0;
        const ui32 PartitionsCount = 0;
        TMaybe<TIssues> Issues;
    };
};

} // anonymous namespace

class TMessageStreamReadActor : public TActor<TMessageStreamReadActor>, public IDqComputeActorAsyncInput {
    static constexpr ui64 HealthCheckWakeupTag = 1;
    static constexpr TDuration CHECK_HANGING_PERIOD = TDuration::Minutes(1);

    struct TMetrics {
        TMetrics(
            const TTxId& txId,
            ui64 taskId,
            const ::NMonitoring::TDynamicCounterPtr& counters,
            const TMessageStreamReadActorSettings& settings,
            bool enableStreamingQueriesCounters,
            bool enableCountersPerTask = false)
            : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
            , Counters(counters)
        {
            if (counters) {
                SubGroup = Counters->GetSubgroup("source", settings.MetricsSource);
            } else {
                SubGroup = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            }

            Source = SubGroup;
            Task = SubGroup;
            if (enableStreamingQueriesCounters) {
                for (const auto& [label, value] : settings.SensorLabels) {
                    SubGroup = SubGroup->GetSubgroup(label, value);
                }
                Source = SubGroup->GetSubgroup("tx_id", TxId);
                if (enableCountersPerTask) {
                    Task = Source->GetSubgroup("task_id", ToString(taskId));
                } else {
                    Task = Source;
                }
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
        ui32 Index = 0;
        TMessageStreamReadCluster Config;
        std::shared_ptr<NFq::IMessageStreamClient> TopicClient;
        std::shared_ptr<NFq::IMessageStreamReadSession> ReadSession;
        NThreading::TFuture<std::shared_ptr<NFq::IMessageStreamReadSession>> PendingSession;
        NThreading::TFuture<void> EventFuture;
        bool SubscribedOnEvent = false;
        TMaybe<TInstant> WaitEventStartedAt;
    };

public:
    static constexpr char ActorName[] = "DQ_MESSAGE_STREAM_READ_ACTOR";

    TMessageStreamReadActor(TMessageStreamReadActorSettings settings,
        std::unique_ptr<IMessageStreamReadActorState> state)
        : TActor<TMessageStreamReadActor>(&TMessageStreamReadActor::StateFunc)
        , Settings(std::move(settings))
        , CpuQuota(Settings.WorkFactory)
        , State(std::move(state))
        , Partitions(State->GetReadState().Partitions)
        , IngressStats(State->GetReadState().IngressStats)
        , StartingMessageTimestamp(State->GetReadState().StartingMessageTimestamp)
        , InputIndex(Settings.InputIndex)
        , ComputeActorId(Settings.ComputeActorId)
        , TxId(Settings.TxId)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", task: " << Settings.TaskId << ". MessageStream source. ")
        , ReconnectPeriod(Settings.ReconnectPeriod)
        , Metrics(TxId, Settings.TaskId, Settings.Counters, Settings, Settings.EnableStreamingQueriesCounters)
        , BufferSize(Settings.BufferSize)
        , Alloc(Settings.Alloc)
        , MetadataFields(Settings.MetadataFields)
        , WithoutConsumer(Settings.Consumer.empty())
        , CheckPartitionCountPeriod(Settings.CheckPartitionCountPeriod)
        , BeginOffset(Settings.BeginOffset), EndOffset(Settings.EndOffset)
        , BeginWriteTime(Settings.BeginWriteTime), EndWriteTime(Settings.EndWriteTime)
    {
        Y_ENSURE(Alloc, "Message stream read actor requires an allocator");
        InitWatermarkTracker();
        IngressStats.Level = Settings.StatsLevel;
        if (BeginWriteTime && StartingMessageTimestamp < *BeginWriteTime) {
            StartingMessageTimestamp = *BeginWriteTime;
        }
    }

    ~TMessageStreamReadActor() override {
        CloseSessions();
        TGuard<TScopedAlloc> guard(*Alloc);
        ClearMkqlData();
    }

    TDuration GetCpuTime() override { return CpuQuota.GetCpuTime(); }

    ui64 GetInputIndex() const override { return InputIndex; }
    const TDqAsyncStats& GetIngressStats() const override { return IngressStats; }

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
        State->SaveState(checkpoint, state);
        state.InputIndex = InputIndex;
        if (!WithoutConsumer) {
            DeferredCommits.emplace_back(checkpoint.GetId(), std::move(CurrentDeferredCommit));
            CurrentDeferredCommit = {};
        }
    }

    void LoadState(const TSourceState& state) override {
        CloseSessions();
        State->StopConsumerOffsetInitialization();
        ClearMkqlData();
        DeferredCommits = {};
        CurrentDeferredCommit = {};
        ActivePartitionSessions.clear();
        StoppingPartitions.clear();
        ExhaustedPartitions.clear();
        FinishedPartitions.clear();
        FinishedByOffsets = false;
        State->LoadState(state);
        InitWatermarkTracker();
        Clusters.clear();
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        if (!WithoutConsumer) {
            while (!DeferredCommits.empty() && DeferredCommits.front().first <= checkpoint.GetId()) {
                DeferredCommits.front().second.Commit();
                DeferredCommits.pop_front();
            }
        }
        ConfirmStoppedPartitions();
    }

    NFq::IMessageStreamClient& GetTopicClient(TClusterState& cluster) {
        if (!cluster.TopicClient) {
            cluster.TopicClient = cluster.Config.CreateClient(ActorContext());
        }
        return *cluster.TopicClient;
    }

    NFq::IMessageStreamReadSession* GetReadSession(TClusterState& cluster) {
        if (!cluster.ReadSession) {
            if (!cluster.PendingSession.Initialized()) {
                cluster.PendingSession = cluster.Config.CreateSession(ActorContext(), GetTopicClient(cluster), GetMessageStreamReadSettings(cluster));
                if (!cluster.PendingSession.IsReady()) {
                    cluster.PendingSession.Subscribe([system = TActivationContext::ActorSystem(), self = SelfId()](const auto&) {
                        system->Send(self, new TEvPrivate::TEvSourceDataReady());
                    });
                    return nullptr;
                }
            }
            if (!cluster.PendingSession.IsReady()) {
                return nullptr;
            }
            cluster.ReadSession = cluster.PendingSession.GetValue();
            cluster.PendingSession = {};
            Y_ENSURE(cluster.ReadSession, "Read session factory returned null");
            ScheduleStatusRequest();
            if (WatermarkTracker) {
                for (auto id : cluster.Config.Partitions) {
                    WatermarkTracker->RegisterPartition({cluster.Config.Name, id}, TInstant::Now());
                }
            }
        }
        return cluster.ReadSession.get();
    }

    void CloseSessions() {
        for (auto& cluster : Clusters) {
            if (cluster.ReadSession) {
                cluster.ReadSession->Close();
                cluster.ReadSession.reset();
            }
            if (cluster.PendingSession.Initialized()) {
                // A factory may finish after the actor has gone away.
                cluster.PendingSession.Subscribe([](const auto& future) {
                    try { if (auto session = future.GetValue()) { session->Close(); } } catch (...) {}
                });
                cluster.PendingSession = {};
            }
        }
    }

    TString GetSessionId() const {
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
        hFunc(TEvExecuteMessageStreamCallback, HandleCallback);
        hFunc(TEvPrivate::TEvResumeCallbacks, Handle);
        hFunc(TEvPrivate::TEvCheckPartitionTimer, Handle);
        hFunc(TEvPrivate::TEvCheckPartitionCount, Handle);
        hFunc(TEvPrivate::TEvCheckPartitionCountResult, Handle);
        hFunc(TEvPrivate::TEvRequestPartitionStatus, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvInvokeResult, HandleConsumerOffsets);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

    void HandleCallback(TEvExecuteMessageStreamCallback::TPtr& ev) {
        if (!CpuQuotaRegistered) {
            CpuQuota.RegisterForResume(SelfId());
            CpuQuotaRegistered = true;
        }
        const bool wasEmpty = PendingCallbacks.empty();
        auto event = ev->Release();
        PendingCallbacks.emplace(event.Release());
        if (wasEmpty) {
            ExecuteCallback();
        }
    }

    void ExecuteCallback() {
        if (PendingCallbacks.empty()) {
            return;
        }
        const auto delay = CpuQuota.Execute([&] {
            auto event = std::move(PendingCallbacks.front());
            PendingCallbacks.pop();
            event->Execute();
        });
        if (!PendingCallbacks.empty()) {
            auto* resume = new TEvPrivate::TEvResumeCallbacks(++CallbackResumeGeneration);
            if (delay) {
                Schedule(*delay, resume);
            } else {
                Send(SelfId(), resume);
            }
        }
    }

    void Handle(TEvPrivate::TEvResumeCallbacks::TPtr& ev) {
        if (ev->Get()->Generation == CallbackResumeGeneration) {
            CpuQuota.NotifyResumed(false);
            ExecuteCallback();
        }
    }
    void HandleConsumerOffsets(TEvents::TEvInvokeResult::TPtr& event) { State->HandleConsumerOffsets(event); }
    bool ConsumerOffsetsInitialized() const { return State->ConsumerOffsetsInitialized(); }

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
        if (!CaNotified) {
            Metrics.InFlyAsyncInputData->Inc();
            CaNotified = true;
        }

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
        }
        CloseSessions();
        Reconnected = true;
        Metrics.ReconnectRate->Inc();
        // A pending factory has no WaitEvent subscription to wake the reader.
        NotifyCA();

        Schedule(ReconnectPeriod, new TEvPrivate::TEvReconnectSession());
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        CpuQuota.Cancel();
        ++CallbackResumeGeneration;
        PendingCallbacks = {};
        State->StopConsumerOffsetInitialization();
        ClearMkqlData();

        CloseSessions();
        for (auto& clusterState : Clusters) {
            clusterState.TopicClient.reset();
        }
        TActor<TMessageStreamReadActor>::PassAway();
    }

    void StartClusterDiscovery() {
        Y_ENSURE(Clusters.empty());

        ui32 index = 0;
        for (const auto& config : Settings.Clusters) {
            auto& cluster = Clusters.emplace_back();
            cluster.Index = index++;
            cluster.Config = config;
            for (auto partition : config.Partitions) {
                Partitions[TPartitionKey{config.Name, partition}];
            }
            if (!WithoutConsumer) {
                GetTopicClient(cluster);
                State->InitConsumerOffsets(SelfId(), cluster.Index, cluster.TopicClient, config.PartitionsCount);
            }
        }

        Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
        SchedulePartitionCountTimer();
    }

    void Handle(TEvPrivate::TEvRequestPartitionStatus::TPtr&) {
        StatusRequestScheduled = false;
        for (const auto& [key, session] : ActivePartitionSessions) {
            SRC_LOG_D("RequestStatus for partition " << key.PartitionId << " cluster \"" << key.Cluster << "\"");
            session->RequestStatus();
        }
        ScheduleStatusRequest();
    }


    void ScheduleStatusRequest() {
        if (!StatusRequestScheduled
            && !FinishedByOffsets && Settings.StopAtCurrentEndOffsets
            && (BeginWriteTime || EndWriteTime)) {
            StatusRequestScheduled = true;
            Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRequestPartitionStatus());
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != HealthCheckWakeupTag) {
            if (CpuQuota.IsWaiting()) {
                ++CallbackResumeGeneration;
                CpuQuota.NotifyResumed(true);
                ExecuteCallback();
            }
            return;
        }
        WakeupScheduled = false;
        ScheduleWakeup();

        if (TInstant::Now() - LastActiveTime <= CHECK_HANGING_PERIOD) {
            return;
        }
        LastActiveTime = TInstant::Now();

        for (const auto& clusterState : Clusters) {
            if (const auto& readSession = clusterState.ReadSession) {
                SRC_LOG_D("Check Read Session, cluster: " << clusterState.Config.Name << ", is ready: " << readSession->WaitEvent().IsReady());
            }
        }
    }

    void ScheduleWakeup() {
        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(CHECK_HANGING_PERIOD, new TEvents::TEvWakeup(HealthCheckWakeupTag));
        }
    }

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool& finished, i64 freeSpace) override {
        // called with bound allocator
        if (CaNotified) {
            Metrics.InFlyAsyncInputData->Dec();
            CaNotified = false;
        }
        SRC_LOG_T("SessionId: " << GetSessionId() << " GetAsyncInputData freeSpace = " << freeSpace);
        finished = FinishedByOffsets && ReadyBuffer.empty();

        const auto now = TInstant::Now();

        if (!InflightReconnect && ReconnectPeriod != TDuration::Zero()) {
            Metrics.ReconnectRate->Inc();
            Schedule(ReconnectPeriod, new TEvPrivate::TEvReconnectSession());
            InflightReconnect = true;
        }

        if (Reconnected) {
            Reconnected = false;
            ReadyBuffer = std::deque<TReadyBatch>{}; // clear read buffer
        }

        if (freeSpace <= 0) { return 0; }
        i64 usedSpace = 0;
        if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
            finished = FinishedByOffsets && ReadyBuffer.empty();
            return usedSpace;
        }

        bool recheckBatch = false;
        if (freeSpace > 0 && !FinishedByOffsets) {
            if (Clusters.empty()) {
                StartClusterDiscovery();
            }
            const bool consumerOffsetsInitialized = ConsumerOffsetsInitialized();
            for (auto& clusterState : Clusters) {
                if (clusterState.Config.PartitionsCount == 0 || !consumerOffsetsInitialized) {
                    continue;
                }
                auto* session = GetReadSession(clusterState);
                if (!session) { continue; }
                auto events = session->GetEvents({
                    .Block = false,
                    .MaxByteSize = static_cast<size_t>(freeSpace),
                });
                if (!events.empty()) {
                    recheckBatch = true;
                }

                ui32 batchItemsEstimatedCount = 0;
                for (auto& event : events) {
                    if (const auto* val = std::get_if<NFq::TMessageStreamDataEvent>(&event)) {
                        batchItemsEstimatedCount += val->Records.size();
                    }
                }

                TTopicEventProcessor topicEventProcessor {*this, clusterState, batchItemsEstimatedCount, LogPrefix, TString(clusterState.Config.Name), clusterState.Index };
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
                finished = FinishedByOffsets && ReadyBuffer.empty();
            return usedSpace;
            }
        }

        ConfirmStoppedPartitions();
        watermark = Nothing();
        buffer.clear();
        finished = FinishedByOffsets && ReadyBuffer.empty();
        return 0;
    }

    void CheckFinishedByOffsets() {
        if (!Settings.StopAtCurrentEndOffsets
            || Clusters.empty()
            || FinishedByOffsets) {
            return;
        }
        if (Partitions.size() != FinishedPartitions.size()) {
            return;
        }
        SRC_LOG_I("SessionId: " << GetSessionId() << ", Finish by offsets");
        FinishedByOffsets = true;

        // Keep the session alive for checkpoint acknowledgements. PassAway
        // closes it once the compute actor has finished with this input.
        Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
    }

    const std::vector<ui64>& GetPartitionsToRead(const TClusterState& cluster) const { return cluster.Config.Partitions; }

    void InitWatermarkTracker() {
        WatermarkTracker.Clear();
        if (Settings.WatermarksEnabled) {
            WatermarkTracker.ConstructInPlace(Settings.WatermarkGranularity, Settings.IdlePartitionsEnabled,
                Settings.LateArrivalDelay, Settings.IdleTimeout, LogPrefix, Metrics.Counters ? Metrics.Source : nullptr);
        }
    }

    void MaybeSchedulePartitionIdlenessCheck(TInstant now) {
        if (const auto next = WatermarkTracker->PrepareIdlenessCheck(now)) {
            Schedule(*next, new TEvPrivate::TEvPartitionIdleness(*next));
        }
    }

    bool HasReadTimeLowerBound() const {
        // Zero is the legacy checkpoint representation of an absent bound.
        // An explicit BeginWriteTime also preserves a requested epoch bound.
        return StartingMessageTimestamp != TInstant::Zero() || BeginWriteTime.Defined();
    }

    bool RequiresWriteTime() const {
        return Settings.RequireWriteTime || Settings.WatermarksEnabled
            || HasReadTimeLowerBound() || EndWriteTime.Defined();
    }

    NFq::TMessageStreamReadSessionSettings GetMessageStreamReadSettings(TClusterState& clusterState) const {
        NFq::TMessageStreamReadSessionSettings settings;
        for (const auto partitionId : GetPartitionsToRead(clusterState)) {
            settings.PartitionIds.push_back(NFq::TMessageStreamPartitionId{partitionId});
        }
        if (HasReadTimeLowerBound()) {
            settings.ReadFromWriteTime = StartingMessageTimestamp;
        }
        settings.RequireWriteTime = RequiresWriteTime();
        settings.MaxMemoryUsageBytes = BufferSize;
        settings.TraceId = LogPrefix;
        settings.AutoPartitioningSupport = !Settings.StopAtCurrentEndOffsets;
        if (!WithoutConsumer) {
            settings.Consumer = Settings.Consumer;
        }
        return settings;
    }


    static TPartitionKey MakePartitionKey(const TString& cluster, const std::shared_ptr<NFq::IMessageStreamPartitionControl>& partition) {
        Y_DEBUG_ABORT_UNLESS(partition, "Missing partition session for partition key creation");
        return { cluster, partition->GetPartitionId().Value };
    }

    static TPartitionKey MakePartitionKey(const TString& cluster, ui64 partitionId) {
        return { cluster, partitionId };
    }

    void SubscribeOnNextEvent() {
        if (FinishedByOffsets || !ConsumerOffsetsInitialized()) {
            return;
        }
        for (auto& clusterState : Clusters) {
            SubscribeOnNextEvent(clusterState);
        }
    }

    void SubscribeOnNextEvent(TClusterState& clusterState) {
        if (!clusterState.Config.PartitionsCount) {
            return;
        }
        auto* session = GetReadSession(clusterState);
        if (!session) { return; }
        if (!clusterState.SubscribedOnEvent) {
            clusterState.SubscribedOnEvent = true;
            Metrics.InFlySubscribe->Inc();
            TActorSystem* actorSystem = TActivationContext::ActorSystem();
            clusterState.WaitEventStartedAt = TInstant::Now();
            clusterState.EventFuture = session->WaitEvent().Subscribe([actorSystem, selfId = SelfId(), index = clusterState.Index](const auto&){
                actorSystem->Send(selfId, new TEvPrivate::TEvSourceDataReady(), 0, 1 + index);
            });
        }
    }

    void ConfirmStoppedPartitions() {
        if (StoppingPartitions.empty() && ExhaustedPartitions.empty()) {
            return;
        }

        // Track assignments, not partition IDs: an old and a new assignment
        // of the same partition must not hold up each other's confirmations.
        using TControl = std::shared_ptr<NFq::IMessageStreamPartitionControl>;
        std::set<TControl, std::owner_less<TControl>> pending;
        const auto collect = [&pending](const auto& ranges) {
            for (const auto& [control, _] : ranges) {
                pending.insert(control);
            }
        };
        for (const auto& batch : ReadyBuffer) {
            collect(batch.OffsetRanges);
        }
        collect(CurrentDeferredCommit.Ranges);
        for (const auto& [_, commit] : DeferredCommits) {
            collect(commit.Ranges);
        }

        std::erase_if(StoppingPartitions, [&](const auto& control) {
            if (pending.contains(control)) {
                return false;
            }
            control->ConfirmStop();
            return true;
        });
        std::erase_if(ExhaustedPartitions, [&](const auto& control) {
            if (pending.contains(control)) {
                return false;
            }
            control->ConfirmExhausted();
            return true;
        });
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
        std::map<std::shared_ptr<NFq::IMessageStreamPartitionControl>, std::pair<std::string, TList<std::pair<ui64, ui64>>>, std::owner_less<>> OffsetRanges; // [start, end)
        TInstant LastWriteTime;
    };

    // must be called with bound allocator
    bool MaybeReturnReadyBatch(TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, i64& usedSpace) {
        if (ReadyBuffer.empty()) {
            CheckFinishedByOffsets();
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
            auto key = MakePartitionKey(TString(cluster), partitionSession);
            auto& partitionInfo = Partitions[key];
            partitionInfo.Offset = ranges.back().second;
            partitionInfo.LastMessageWriteTime = readyBatch.LastWriteTime;
            if (Settings.StopAtCurrentEndOffsets && partitionInfo.IsFinishedInTableMode()) {
                FinishedPartitions.insert(key);
            }
        }

        ReadyBuffer.pop_front();
        ConfirmStoppedPartitions();

        if (ReadyBuffer.empty()) {
            CheckFinishedByOffsets();
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
            ReadyBuffer.emplace_back(watermark, 0);
            return;
        }

        ReadyBuffer.back().Watermark = watermark;
    }

    // must be called with bound allocator
    void ClearMkqlData() {
        std::deque<TReadyBatch> empty;
        ReadyBuffer.swap(empty);
    }

    void SchedulePartitionCountTimer() {
        if (!CheckPartitionCountPeriod || Settings.StopAtCurrentEndOffsets || PartitionCountTimerScheduled) {
            return;
        }
        PartitionCountTimerScheduled = true;
        Schedule(CheckPartitionCountPeriod, new TEvPrivate::TEvCheckPartitionTimer());
    }

    void Handle(TEvPrivate::TEvCheckPartitionTimer::TPtr& /*ev*/) {
        PartitionCountTimerScheduled = false;
        SchedulePartitionCountTimer();

        for (auto& clusterState : Clusters) {
            const auto checkTime = CheckPartitionCountPeriod * RandomNumber<double>();
            SRC_LOG_T("Next partition count check in " << checkTime << " seconds (cluster \"" << clusterState.Config.Name << "\")");
            Schedule(checkTime, new TEvPrivate::TEvCheckPartitionCount(clusterState.Index));
        }
    }

    void Handle(TEvPrivate::TEvCheckPartitionCount::TPtr& ev) {
        auto& clusterState = Clusters[ev->Get()->ClusterIndex];
        SRC_LOG_T("Checking partition count for topic \"" << Settings.Stream << "\", cluster \"" << clusterState.Config.Name << "\"");

        GetTopicClient(clusterState)
            .DescribeStream()
            .Subscribe([
                index = clusterState.Index,
                actorSystem = TActivationContext::ActorSystem(),
                selfId = SelfId()](const auto& describeTopicFuture)
            {
                try {
                    const auto& describeTopic = describeTopicFuture.GetValue();
                    if (!describeTopic.IsSuccess()) {
                        actorSystem->Send(selfId, new TEvPrivate::TEvCheckPartitionCountResult(index,
                            describeTopic.Issues));
                        return;
                    }
                    actorSystem->Send(selfId, new TEvPrivate::TEvCheckPartitionCountResult(index, static_cast<ui32>(describeTopic.Value.Partitions.size())));
                } catch (const std::exception& ex) {
                    actorSystem->Send(selfId, new TEvPrivate::TEvCheckPartitionCountResult(index,
                        TIssues{TIssue(ex.what())}
                    ));
                }
            });
    }

    void Handle(TEvPrivate::TEvCheckPartitionCountResult::TPtr& ev) {
        auto clusterIndex = ev->Get()->ClusterIndex;
        auto partitionsCount = ev->Get()->PartitionsCount;

        if (ev->Get()->Issues) {
            SRC_LOG_W("Periodic DescribeTopic failed for topic \"" << Settings.Stream << "\""
                << " on cluster index " << clusterIndex << ": " << ev->Get()->Issues->ToOneLineString());
            return;
        }
        if (clusterIndex < Clusters.size() && Clusters[clusterIndex].Config.PartitionsCount != partitionsCount) {
            TStringBuilder message;
            message << "Number of partitions in the topic \"" << Settings.Stream << "\"";
            if (!Clusters[clusterIndex].Config.Name.empty()) {
                message << " (on cluster \"" << Clusters[clusterIndex].Config.Name << "\")";
            }
            message << " is changed from " << Clusters[clusterIndex].Config.PartitionsCount << " to " << partitionsCount
                << ". You need to restart (alter with text or drop / create) query to read all partitions.";
            SRC_LOG_E(message);
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, TIssues({TIssue(message)}), NYql::NDqProto::StatusIds::SCHEME_ERROR));
            return;
        }
    }

    // must be called (visited) with bound allocator
    struct TTopicEventProcessor {
        void operator()(NFq::TMessageStreamDataEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);
            auto& partitionInfo = Self.Partitions[partitionKey];

            for (const auto& record : event.Records) {
                if (record.DecompressionError) {
                    ythrow yexception() << "Failed to decompress message at offset " << record.Id.Offset << ": " << *record.DecompressionError;
                }
                if (!record.Data) {
                    ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                        << "MessageStream reader does not support null message payloads";
                }
                const TString& data = *record.Data;
                Self.IngressStats.Bytes += data.size();
                if (Self.Settings.TraceRecord) { Self.Settings.TraceRecord(record); }
                SRC_LOG_T("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Data received, offset " << record.Id.Offset);

                bool needSkip = false;
                if (Self.Settings.StopAtCurrentEndOffsets && partitionInfo.EndOffset && *partitionInfo.EndOffset <= record.Id.Offset) {
                    SRC_LOG_T("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Skip data. Message offset: " << record.Id.Offset << ", end offset: " << *partitionInfo.EndOffset << ")");
                    needSkip = true;
                }

                if (Self.RequiresWriteTime() && !record.WriteTime) {
                    ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                        << "MessageStream reader requires backend message write time";
                }
                const auto partitionTime = record.WriteTime.value_or(TInstant::Zero());
                if (Self.Settings.StopAtCurrentEndOffsets && partitionInfo.EndWriteTime && *partitionInfo.EndWriteTime <= partitionTime) {
                    SRC_LOG_T("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Skip data. Message writetime: " << partitionTime << ", end write time: " << *partitionInfo.EndWriteTime << ")");
                    needSkip = true;
                }

                if (ClusterState.Config.AdvancePartitionTime) {
                    ClusterState.Config.AdvancePartitionTime(event.PartitionControl->GetPartitionId().Value, partitionTime);
                }

                if (partitionTime < Self.StartingMessageTimestamp) {
                    SRC_LOG_T("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " Skip data. StartingMessageTimestamp: " << Self.StartingMessageTimestamp << ". Write time: " << partitionTime);
                    needSkip = true;
                }

                if (Self.ReadyBuffer.empty() || Self.ReadyBuffer.back().Watermark.Defined()) {
                    Self.ReadyBuffer.emplace_back(Nothing(), BatchCapacity);
                }
                TReadyBatch& activeBatch = Self.ReadyBuffer.back();

                if (!needSkip) {
                    auto [item, size] = CreateItem(record);
                    activeBatch.Data.emplace_back(std::move(item));
                    activeBatch.UsedSpace += size;
                }
                activeBatch.LastWriteTime = partitionTime;

                auto& [cluster, offsets] = activeBatch.OffsetRanges[event.PartitionControl];
                cluster = Cluster;

                if (!offsets.empty() && offsets.back().second == record.Id.Offset) {
                    offsets.back().second = record.Id.Offset + 1;
                } else {
                    offsets.emplace_back(record.Id.Offset, record.Id.Offset + 1);
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

        void operator()(NFq::TMessageStreamSessionClosedEvent& ev) {
            const auto& LogPrefix = Self.LogPrefix;
            TString message = (TStringBuilder() << "Read session to topic \"" << Self.Settings.Stream << "\" was closed");
            SRC_LOG_E("SessionId: " << Self.GetSessionId(Index) << " " << message << ": " << ev.Issues.ToOneLineString());
            TIssue issue(message);
            for (const auto& subIssue : ev.Issues) {
                issue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));
            }
            Self.Send(Self.ComputeActorId, new TEvAsyncInputError(Self.InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
        }

        void operator()(NFq::TMessageStreamPartitionStartRequestedEvent& event) {
            if (Self.Settings.StopAtCurrentEndOffsets && !event.EndOffset) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "MessageStream reader requires a partition end offset";
            }
            const auto endOffset = event.EndOffset.value_or(0);
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);

            Self.ActivePartitionSessions[partitionKey] = event.PartitionControl;

            auto& partitionInfo = Self.Partitions[partitionKey];
            if (!partitionInfo.Offset && Self.BeginOffset) {
                partitionInfo.Offset = *Self.BeginOffset;
            }
            if (!partitionInfo.Offset && event.CommittedOffset) {
                partitionInfo.Offset = event.CommittedOffset;
            }
            partitionInfo.EndWriteTime = Self.EndWriteTime;

            if (!Self.Settings.StopAtCurrentEndOffsets
                && partitionInfo.Offset
                && event.EndOffset && *partitionInfo.Offset > endOffset) {
                TStringBuilder message;
                message << "Requested offsets do not exist in the topic \"" << Self.Settings.Stream
                    << "\": offset " << *partitionInfo.Offset << " for partition " << partitionKey.PartitionId
                    << " exceeds the end offset " << endOffset
                    << ". The topic may have been recreated. Recreate or restart the streaming query.";
                SRC_LOG_E("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " " << message);
                Self.Send(Self.ComputeActorId, new TEvAsyncInputError(
                    Self.InputIndex,
                    TIssues({TIssue(message)}),
                    NYql::NDqProto::StatusIds::BAD_REQUEST));
                return;
            }

            if (event.EndOffset && !partitionInfo.EndOffset) {
                partitionInfo.EndOffset = endOffset;
                if (Self.EndOffset && *Self.EndOffset < *partitionInfo.EndOffset) {
                    *partitionInfo.EndOffset = *Self.EndOffset;
                    SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " End offset was changed to " << *partitionInfo.EndOffset);
                }

                if (Self.Settings.StopAtCurrentEndOffsets && partitionInfo.IsFinishedInTableMode()) {
                    Self.FinishedPartitions.insert(partitionKey);
                }
            }

            std::optional<uint64_t> maxOffset;
            if (Self.Settings.StopAtCurrentEndOffsets && partitionInfo.EndOffset && *partitionInfo.EndOffset) {
                maxOffset = *partitionInfo.EndOffset - 1;
            }

            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << "StartPartitionSessionEvent received (end offset " << endOffset
                << "), confirm StartPartitionSession with start offset " << (partitionInfo.Offset ? ToString(*partitionInfo.Offset) : "<null>")
                << ", max offset " << (maxOffset ? ToString(*maxOffset) : "<null>"));
            event.PartitionControl->ConfirmStart(partitionInfo.Offset, maxOffset);
        }

        void operator()(NFq::TMessageStreamPartitionStopRequestedEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " StopPartitionSessionEvent received");
            auto it = Self.ActivePartitionSessions.find(partitionKey);
            if (it != Self.ActivePartitionSessions.end() && it->second == event.PartitionControl) {
                Self.ActivePartitionSessions.erase(it);
            }
            Self.StoppingPartitions.push_back(event.PartitionControl);
        }

        void operator()(NFq::TMessageStreamPartitionExhaustedEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " EndPartitionSessionEvent received");
            if (!Self.Settings.EnableStreamingAutopartitioning && !Self.Settings.StopAtCurrentEndOffsets) {
                TStringBuilder message;
                message << "Topic (" << Self.Settings.Stream << ") with auto partitioning is not supported.";
                SRC_LOG_E(message);
                Self.Send(Self.ComputeActorId, new TEvAsyncInputError(Self.InputIndex, TIssues({TIssue(message)}), NYql::NDqProto::StatusIds::SCHEME_ERROR));
            } else {
                Self.ExhaustedPartitions.push_back(event.PartitionControl);
            }
        }

        void operator()(NFq::TMessageStreamPartitionStatusEvent& event) {
            const auto& LogPrefix = Self.LogPrefix;
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);
            const auto active = Self.ActivePartitionSessions.find(partitionKey);
            if (active == Self.ActivePartitionSessions.end() || active->second != event.PartitionControl) {
                return;
            }
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey
                << " PartitionSessionStatusEvent:"
                << " CommittedOffset=" << (event.CommittedOffset ? ToString(*event.CommittedOffset) : "<unknown>")
                << " ReadOffset=" << (event.ReadOffset ? ToString(*event.ReadOffset) : "<unknown>")
                << " EndOffset=" << (event.EndOffset ? ToString(*event.EndOffset) : "<unknown>")
                << " WriteTimeHighWatermark=" << (event.WriteTimeHighWatermark ? ToString(*event.WriteTimeHighWatermark) : "<none>"));

            if (Self.Settings.StopAtCurrentEndOffsets) {
                auto& partitionInfo = Self.Partitions[partitionKey];
                // Detect that the session will not deliver more messages: server-side read offset
                // reached the end offset that was established at session start.
                // This handles the case where StartingMessageTimestamp (= BeginWriteTime) causes the
                // server to skip all messages internally, so no TDataReceivedEvent ever arrives and
                // partitionInfo.Offset is never updated by MaybeReturnReadyBatch.
                // Closing the session here is safe: any already-buffered data in ReadyBuffer is
                // still delivered to the CA, since CheckFinishedByOffsets only closes the read
                // session but does not clear ReadyBuffer.
                if (partitionInfo.EndOffset && event.ReadOffset && *event.ReadOffset >= *partitionInfo.EndOffset) {
                    SRC_LOG_I("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey
                        << " Partition finished by status check: ReadOffset=" << (event.ReadOffset ? ToString(*event.ReadOffset) : "<unknown>")
                        << " EndOffset=" << *partitionInfo.EndOffset);
                    Self.FinishedPartitions.insert(partitionKey);
                    Self.CheckFinishedByOffsets();
                }
            }
        }

        void operator()(NFq::TMessageStreamPartitionClosedEvent& event) {
            const auto partitionKey = MakePartitionKey(Cluster, event.PartitionControl);
            SRC_LOG_D("SessionId: " << Self.GetSessionId(Index) << " Key: " << partitionKey << " PartitionSessionClosedEvent received");
            auto it = Self.ActivePartitionSessions.find(partitionKey);
            if (it != Self.ActivePartitionSessions.end() && it->second == event.PartitionControl) {
                Self.ActivePartitionSessions.erase(it);
            }
        }

        std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const NFq::TMessageStreamRecord& record) {
            if (!record.Data) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "MessageStream reader does not support null message payloads";
            }
            const TString& data = *record.Data;

            i64 usedSpace = 0;
            NUdf::TUnboxedValuePod item;
            if (Self.MetadataFields.empty()) {
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
                usedSpace += data.size();
            } else {
                NUdf::TUnboxedValue* itemPtr;
                item = Self.Settings.HolderFactory->CreateDirectArrayHolder(Self.MetadataFields.size() + 1, itemPtr);
                *(itemPtr++) = MakeString(NUdf::TStringRef(data.data(), data.size()));
                usedSpace += data.size();

                for (const auto& extractor : Self.MetadataFields) {
                    auto [ub, size] = extractor(record, Cluster);
                    *(itemPtr++) = std::move(ub);
                    usedSpace += size;
                }
            }

            return std::make_pair(item, usedSpace);
        }

        TMessageStreamReadActor& Self;
        TClusterState& ClusterState;
        ui32 BatchCapacity;
        const TString& LogPrefix;
        const TString Cluster;
        const ui32 Index;
    };

private:
    const TMessageStreamReadActorSettings Settings;
    NInternal::TMessageStreamCpuQuota CpuQuota;
    bool CpuQuotaRegistered = false;
    ui64 CallbackResumeGeneration = 0;
    std::queue<std::unique_ptr<TEvExecuteMessageStreamCallback>> PendingCallbacks;
    const std::unique_ptr<IMessageStreamReadActorState> State;
    THashMap<TPartitionKey, TPartitionProgress>& Partitions;
    TDqAsyncStats& IngressStats;
    TInstant& StartingMessageTimestamp;
    const ui64 InputIndex;
    const TActorId ComputeActorId;
    const TTxId TxId;
    const TString LogPrefix;
    TMaybe<TDqSourceWatermarkTracker<TPartitionKey>> WatermarkTracker;
    bool InflightReconnect = false;
    TDuration ReconnectPeriod;
    bool Reconnected = false;
    TMetrics Metrics;
    const i64 BufferSize;
    const std::shared_ptr<TScopedAlloc> Alloc;
    std::vector<TClusterState> Clusters;
    struct TStreamDeferredCommit {
        using TPartitionControl = std::shared_ptr<NFq::IMessageStreamPartitionControl>;

        void Add(TPartitionControl partition, ui64 start, ui64 end) {
            Y_ENSURE(partition);
            Y_ENSURE(start < end, "Empty or reversed commit interval");
            auto& ranges = Ranges[std::move(partition)];
            Y_ENSURE(!ranges.Intersects(start, end), "Overlapping commit intervals");
            ranges.InsertInterval(start, end);
        }

        void Commit() {
            for (const auto& [partition, ranges] : Ranges) {
                for (const auto& [start, end] : ranges) {
                    // A revoked assignment can reject the acknowledgement. Keep
                    // the checkpoint; uncommitted records may be replayed on reassignment.
                    partition->AcknowledgeRange(start, end);
                }
            }
            Ranges.clear();
        }

        // Keep different partition sessions separate, even for the same partition ID.
        std::map<TPartitionControl, TDisjointIntervalTree<ui64>, std::owner_less<TPartitionControl>> Ranges;
    };

    std::deque<std::pair<ui64, TStreamDeferredCommit>> DeferredCommits;
    TStreamDeferredCommit CurrentDeferredCommit;
    std::vector<TMessageStreamReadActorSettings::TMetaExtractor> MetadataFields;
    std::deque<TReadyBatch> ReadyBuffer;
    bool WithoutConsumer = false;
    bool WakeupScheduled = false;
    TInstant LastActiveTime = TInstant::Now();
    bool CaNotified = false;
    bool FinishedByOffsets = false;
    THashSet<TPartitionKey> FinishedPartitions;
    THashMap<TPartitionKey, std::shared_ptr<NFq::IMessageStreamPartitionControl>> ActivePartitionSessions;
    std::vector<std::shared_ptr<NFq::IMessageStreamPartitionControl>> StoppingPartitions, ExhaustedPartitions;
    bool StatusRequestScheduled = false;
    const TDuration CheckPartitionCountPeriod;
    TInstant NextCheckPartitionTime = TInstant::Now();
    bool PartitionCountTimerScheduled = false;
    TMaybe<ui64> BeginOffset;
    TMaybe<ui64> EndOffset;
    TMaybe<TInstant> BeginWriteTime;
    TMaybe<TInstant> EndWriteTime;
};

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateMessageStreamReadActor(
    TMessageStreamReadActorSettings settings, std::unique_ptr<IMessageStreamReadActorState> state) {
    auto* actor = new TMessageStreamReadActor(std::move(settings), std::move(state));
    return {actor, actor};
}
} // namespace NFq::NMessageStream
