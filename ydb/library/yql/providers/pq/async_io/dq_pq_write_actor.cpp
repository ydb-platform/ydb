#include "dq_pq_write_actor.h"
#include "probes.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/common/pq_events_processor.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <algorithm>
#include <queue>
#include <variant>

#define SINK_LOG_T(s) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_I(s) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_N(s) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_W(s) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix() << s)
#define SINK_LOG_C(s) LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix() << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr::NMiniKQL;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvPqEventsReady = EvBegin,
        EvExecuteTopicEvent,
        EvDeferredPublicationCreated,
        EvDeferredPublicationCommitted,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events

    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {
    };

    struct TEvExecuteTopicEvent : public TTopicEventBase<TEvExecuteTopicEvent, EvExecuteTopicEvent> {
        using TTopicEventBase::TTopicEventBase;
    };

    template <typename TResult, ui32 EEventId>
    struct TEvApiResult : public TEventLocal<TEvApiResult<TResult, EEventId>, EEventId> {
        explicit TEvApiResult(TResult result, const TInstant startedAt)
            : Result(std::move(result))
            , Latency(TInstant::Now() - startedAt)
        {}

        const TResult Result;
        const TDuration Latency;
    };

    using TEvDeferredPublicationCreated = TEvApiResult<NYdb::NTopic::TBeginPublicationResult, EvDeferredPublicationCreated>;
    using TEvDeferredPublicationCommitted = TEvApiResult<NYdb::NTopic::TPublishResult, EvDeferredPublicationCommitted>;
};

class TDqPqWriteActor final : public TActor<TDqPqWriteActor>, public IActorExceptionHandler, public IDqComputeActorAsyncOutput, TTopicEventProcessor<TEvPrivate::TEvExecuteTopicEvent> {
    static constexpr ui32 STATE_VERSION = 1;
    static constexpr ui32 MAX_MESSAGE_SIZE = 1_MB;
    static constexpr TDuration SLOW_CHECKPOINT_DURATION = TDuration::Minutes(1);

    using TBase = TActor<TDqPqWriteActor>;

    struct TMetrics {
        TMetrics(
            const TTxId& txId, const ui64 taskId, NMonitoring::TDynamicCounterPtr counters, const bool enableStreamingQueriesCounters,
            const bool enableCountersPerTask, const bool enableDeferredPublication
        )
            : TxId(std::visit([](const auto& arg) {
                return ToString(arg);
            }, txId))
            , SubGroup(counters ? counters->GetSubgroup("sink", "PqSink") : MakeIntrusive<NMonitoring::TDynamicCounters>())
        {
            auto task = SubGroup;

            if (enableStreamingQueriesCounters) {
                task = task->GetSubgroup("tx_id", TxId);

                if (enableCountersPerTask) {
                    task = task->GetSubgroup("task_id", ToString(taskId));
                }
            }

            LastAckLatency = task->GetCounter("LastAckLatencyMs");
            InFlyCheckpoints = task->GetCounter("InFlyCheckpoints");
            InFlyPendingAckCheckpoints = task->GetCounter("InFlyPendingAckCheckpoints");
            InFlyPendingCommitCheckpoints = task->GetCounter("InFlyPendingCommitCheckpoints");
            InFlyData = task->GetCounter("InFlyData");
            AlreadyWritten = task->GetCounter("AlreadyWritten");
            FirstContinuationTokenMs = task->GetCounter("FirstContinuationTokenMs");
            EgressDataRate = task->GetCounter("EgressDataRate", true);

            if (enableDeferredPublication) {
                LastBeginPublicationLatency = task->GetCounter("DeferredPublication/LastBeginLatencyMs");
                LastPublishLatency = task->GetCounter("DeferredPublication/LastPublishLatencyMs");
                LastPublicationActiveDuration = task->GetCounter("DeferredPublication/LastActiveDurationMs");
                InFlyActivePublications = task->GetCounter("DeferredPublication/InFlyActive");
                InFlyPendingCommitPublications = task->GetCounter("DeferredPublication/InFlyPendingCommit");
                UnpublishedDataSize = task->GetCounter("DeferredPublication/UnpublishedDataSize");
            }
        }

        ~TMetrics() {
            SubGroup->RemoveSubgroup("tx_id", TxId);
        }

        void ReportFirstContinuationToken() const {
            if (*FirstContinuationTokenMs == 0) {
                FirstContinuationTokenMs->Set((TInstant::Now() - StartTime).MilliSeconds());
            }
        }

        // Common counters
        NMonitoring::TDynamicCounters::TCounterPtr LastAckLatency;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyCheckpoints;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyPendingAckCheckpoints;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyPendingCommitCheckpoints;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyData;
        NMonitoring::TDynamicCounters::TCounterPtr AlreadyWritten;
        NMonitoring::TDynamicCounters::TCounterPtr EgressDataRate;

        // Deferred publication counters
        NMonitoring::TDynamicCounters::TCounterPtr LastBeginPublicationLatency;
        NMonitoring::TDynamicCounters::TCounterPtr LastPublishLatency;
        NMonitoring::TDynamicCounters::TCounterPtr LastPublicationActiveDuration;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyActivePublications;
        NMonitoring::TDynamicCounters::TCounterPtr InFlyPendingCommitPublications;
        NMonitoring::TDynamicCounters::TCounterPtr UnpublishedDataSize;

    private:
        const TInstant StartTime = TInstant::Now();
        const TString TxId;
        const NMonitoring::TDynamicCounterPtr SubGroup;

        NMonitoring::TDynamicCounters::TCounterPtr FirstContinuationTokenMs;
    };

    // Store for messages to write and acks for inflight writes into topic
    class TDataBuffer {
        struct TAckInfo {
            TAckInfo(const i64 messageSize, const ui64 seqNo)
                : MessageSize(messageSize)
                , SeqNo(seqNo)
            {}

            const i64 MessageSize = 0;
            const ui64 SeqNo = 0;
            const TInstant StartTime = TInstant::Now();
        };

    public:
        TDataBuffer(TDqAsyncStats& egressStats, const TMetrics& metrics, const bool enableDeduplication)
            : Metrics(metrics)
            , EnableDeduplication(enableDeduplication)
            , EgressStats(egressStats)
        {}

        bool Empty() const {
            return Messages.empty() && Inflight.empty();
        }

        ui64 GetLastSentMessageSeqNo() const {
            Y_VALIDATE(NextMessageSeqNo > 0, "Unexpected next message seq no: " << NextMessageSeqNo);
            return NextMessageSeqNo - 1;
        }

        ui64 GetLastBufferedSeqNo() const {
            return GetLastSentMessageSeqNo() + Messages.size();
        }

        void PushMessage(TString&& message) {
            Messages.emplace(std::move(message));
            Metrics.InFlyData->Inc();
        }

        std::pair<std::optional<uint64_t>, TString> PopMessage() {
            Y_VALIDATE(!Messages.empty(), "Unexpected empty messages queue");
            auto message = std::move(Messages.front());
            Messages.pop();

            const std::optional<uint64_t> seqNo = EnableDeduplication ? std::optional(NextMessageSeqNo) : std::nullopt;
            const auto itemSize = GetItemSize(message);
            Inflight.emplace(itemSize, NextMessageSeqNo++);
            EgressStats.Bytes += itemSize;
            Metrics.EgressDataRate->Add(itemSize);
            return {seqNo, std::move(message)};
        }

        TAckInfo PopAck() {
            Y_VALIDATE(!Inflight.empty(), "Unexpected empty inflight messages queue");
            auto ackInfo = Inflight.front();
            Inflight.pop();

            Metrics.LastAckLatency->Set((TInstant::Now() - ackInfo.StartTime).MilliSeconds());
            Metrics.InFlyData->Dec();

            // Use seqNo stored on our side because without deduplication we do not specify SeqNo on Write().
            // We expecting that acks comes from server in strictly same order as sent messages.
            Y_VALIDATE(ackInfo.SeqNo == ConfirmedSeqNo + 1, "Unexpected ack seq no: " << ackInfo.SeqNo << " expected: " << ConfirmedSeqNo + 1);
            ConfirmedSeqNo = ackInfo.SeqNo;

            return ackInfo;
        }

        // Called on state restoration from checkpoint
        void LoadConfirmedSeqNo(const ui64 confirmedSeqNo) {
            Y_VALIDATE(NextMessageSeqNo == 1, "Unexpected load state after sending some data");
            ConfirmedSeqNo = confirmedSeqNo;
            NextMessageSeqNo = ConfirmedSeqNo + 1;
        }

    private:
        const TMetrics& Metrics;
        const bool EnableDeduplication = false;
        TDqAsyncStats& EgressStats;

        YDB_READONLY_DEF(std::queue<TString>, Messages);
        YDB_READONLY_DEF(std::queue<TAckInfo>, Inflight);
        YDB_READONLY(ui64, ConfirmedSeqNo, 0);
        ui64 NextMessageSeqNo = 1;
    };

    // Store for pending checkpoints and their states. Checkpoint handle phases:
    // 1. Receive checkpoint from input, save seq no range for messages, which was in buffer before this checkpoint
    // 2. Wait for sending all messages with SeqNo <= WaitConfirmedSeqNo and then reset deferred publication, save egress stats
    // 3. Wait for receiving acks on all messages with SeqNo <= WaitConfirmedSeqNo, then save checkpoint state
    // 4. Wait for checkpoint commit request, then perform deferred publication commit
    class TCheckpointsState {
    public:
        struct TCheckpointInfo {
            TCheckpointInfo(const ui64 waitConfirmedSeqNo, NDqProto::TCheckpoint checkpoint, const ui64 deferredPublicationIntId = 0)
                : WaitConfirmedSeqNo(waitConfirmedSeqNo)
                , Checkpoint(std::move(checkpoint))
                , DeferredPublicationIntId(deferredPublicationIntId)
            {}

            bool Equal(const NDqProto::TCheckpoint& checkpointBound) const {
                return Checkpoint.GetGeneration() == checkpointBound.GetGeneration() && Checkpoint.GetId() == checkpointBound.GetId();
            }

            const ui64 WaitConfirmedSeqNo = 0; // Checkpoint state should be saved after getting acks on all messages with SeqNo <= WaitConfirmedSeqNo
            const NDqProto::TCheckpoint Checkpoint;
            const TInstant StartTime = TInstant::Now();
            ui64 EgressBytes = 0;
            ui64 DataSizeUnderCheckpoint = 0;
            ui64 DeferredPublicationIntId = 0;
            bool AllDataSent = false;
            bool AllAcksReceived = false;
        };

        TCheckpointsState(const TDqAsyncStats& egressStats, const TMetrics& metrics)
            : Metrics(metrics)
            , EgressStats(egressStats)
        {}

        bool Empty() const {
            return !PendingCheckpoint;
        }

        TString GetSlowCheckpointDiagnostics(const ui64 sentSeqNo, const ui64 confirmedSeqNo) const {
            Y_VALIDATE(SelfId && SlowCheckpointMonitoringStarted, "Slow checkpoint monitoring is not started");
            TActivationContext::Schedule(SLOW_CHECKPOINT_DURATION, new IEventHandle(SelfId, SelfId, new TEvents::TEvWakeup()));

            if (!PendingCheckpoint) {
                return "";
            }

            const auto duration = TInstant::Now() - PendingCheckpoint->StartTime;
            if (duration < SLOW_CHECKPOINT_DURATION) {
                return "";
            }

            const auto& info = *PendingCheckpoint;
            const auto waitConfirmedSeqNo = info.WaitConfirmedSeqNo;
            auto result = TStringBuilder() << CheckpointLogString(info.Checkpoint)
                << " Slow PQ sink checkpoint. Duration: " << duration.Seconds() << 's'
                << ". Checkpoint seq no: " << waitConfirmedSeqNo
                << (info.DeferredPublicationIntId ? TStringBuilder() << ". Deferred publication internal id: " << info.DeferredPublicationIntId : TStringBuilder())
                << ". Checkpoint data size: " << info.DataSizeUnderCheckpoint;

            if (!info.AllDataSent) {
                result << ". Waiting for writing seq no range [" << sentSeqNo + 1 << ", " << waitConfirmedSeqNo << "]";
            } else if (!info.AllAcksReceived) {
                result << ". Waiting acks for seq no range [" << confirmedSeqNo + 1 << ", " << waitConfirmedSeqNo << "]";
            } else {
                result << ". Waiting commit request from checkpoint coordinator, last checkpoint bound: " << CheckpointLogString(LastCommitCheckpointBound);
            }

            return result;
        }

        void PushCheckpoint(const ui64 waitConfirmedSeqNo, NDqProto::TCheckpoint checkpoint) {
            Y_VALIDATE(!PendingCheckpoint, "Multiple checkpoint inflight is not supported");
            Y_VALIDATE(!LastCommitCheckpointBound || (
                LastCommitCheckpointBound->GetGeneration() < checkpoint.GetGeneration() || (
                    LastCommitCheckpointBound->GetGeneration() == checkpoint.GetGeneration() &&
                    LastCommitCheckpointBound->GetId() < checkpoint.GetId()
                )
            ), "Unexpected new checkpoint: " << CheckpointLogString(checkpoint) << " for last commit checkpoint bound: " << CheckpointLogString(*LastCommitCheckpointBound));
            PendingCheckpoint.emplace(waitConfirmedSeqNo, std::move(checkpoint));

            if (!SlowCheckpointMonitoringStarted) {
                SlowCheckpointMonitoringStarted = true;
                TActivationContext::Schedule(SLOW_CHECKPOINT_DURATION, new IEventHandle(SelfId, SelfId, new TEvents::TEvWakeup()));
            }

            Metrics.InFlyCheckpoints->Inc();
        }

        bool AdvanceSentSeqNo(const ui64 sentSeqNo, ui64 deferredPublicationIntId) {
            if (!PendingCheckpoint || PendingCheckpoint->AllDataSent || PendingCheckpoint->WaitConfirmedSeqNo > sentSeqNo) {
                return false;
            }

            // For correct deferred publication reset, sent message seq no must advance strictly one by one
            Y_VALIDATE(PendingCheckpoint->WaitConfirmedSeqNo == sentSeqNo, "Unexpected sent seq no: " << sentSeqNo << ". Wait confirmed seq no: " << PendingCheckpoint->WaitConfirmedSeqNo);

            PendingCheckpoint->AllDataSent = true;
            PendingCheckpoint->DeferredPublicationIntId = deferredPublicationIntId;
            PendingCheckpoint->EgressBytes = EgressStats.Bytes;
            PendingCheckpoint->DataSizeUnderCheckpoint = EgressStats.Bytes - std::exchange(WrittenDataUnderCheckpoint, EgressStats.Bytes);
            Metrics.InFlyCheckpoints->Dec();
            Metrics.InFlyPendingAckCheckpoints->Inc();
            return true;
        }

        // Returns checkpoint which state should be saved if any
        std::optional<TCheckpointInfo> AdvanceConfirmedSeqNo(const ui64 confirmedSeqNo) {
            if (!PendingCheckpoint || PendingCheckpoint->AllAcksReceived || PendingCheckpoint->WaitConfirmedSeqNo > confirmedSeqNo) {
                return std::nullopt;
            }

            PendingCheckpoint->AllAcksReceived = true;
            Metrics.InFlyPendingAckCheckpoints->Dec();

            if (PendingCheckpoint->DeferredPublicationIntId) {
                Metrics.InFlyPendingCommitCheckpoints->Inc();
                return PendingCheckpoint;
            }

            // There is no need to track checkpoints without deferred publications
            return std::exchange(PendingCheckpoint, std::nullopt);
        }

        // Returns checkpoint which state should be committed if any
        std::optional<TCheckpointInfo> CommitCheckpoints(const NDqProto::TCheckpoint& checkpointBound) {
            LastCommitCheckpointBound = checkpointBound;

            if (RestoredCheckpoint && RestoredCheckpoint->Equal(checkpointBound)) {
                return *std::exchange(RestoredCheckpoint, std::nullopt);
            }

            // We already committed future checkpoint, so restored checkpoint was committed
            RestoredCheckpoint.reset();

            if (!PendingCheckpoint) {
                return std::nullopt;
            }

            // Checkpoint for commit must be either restored or pending checkpoint
            Y_VALIDATE(PendingCheckpoint->Equal(checkpointBound), "Unexpected checkpoint bound: " << CheckpointLogString(checkpointBound) << ", pending checkpoint: " << (PendingCheckpoint ? CheckpointLogString(PendingCheckpoint->Checkpoint) : "<null>"));
            Y_VALIDATE(PendingCheckpoint->AllDataSent && PendingCheckpoint->AllAcksReceived, "Pending checkpoint is not ready for commit");
            Metrics.InFlyPendingCommitCheckpoints->Dec();
            return std::exchange(PendingCheckpoint, std::nullopt);
        }

        // Called on state restoration from checkpoint
        void LoadPendingCommitCheckpoint(const ui64 confirmedSeqNo, NDqProto::TCheckpoint checkpoint, const ui64 deferredPublicationIntId) {
            Y_VALIDATE(!RestoredCheckpoint, "Cannot load state twice");
            RestoredCheckpoint.emplace(confirmedSeqNo, std::move(checkpoint), deferredPublicationIntId);
        }

    private:
        const TMetrics& Metrics;
        const TDqAsyncStats& EgressStats;
        YDB_ACCESSOR_DEF(TActorId, SelfId);

        std::optional<TCheckpointInfo> PendingCheckpoint;
        std::optional<TCheckpointInfo> RestoredCheckpoint;
        YDB_ACCESSOR(ui64, WrittenDataUnderCheckpoint, 0);
        bool SlowCheckpointMonitoringStarted = false;
        std::optional<NDqProto::TCheckpoint> LastCommitCheckpointBound;
    };

    // Deferred publication lifecycle:
    // 1. Created when first message arrived and work until all data for current checkpoint was not written
    // 2. When checkpoint receive commit request publication will be committed
    // 3. After successful commit checkpoint marked as committed
    class TDeferredPublishState {
        struct TInflightPublicationInfo {
            TInflightPublicationInfo(const ui64 publicationIntId, std::optional<NDqProto::TCheckpoint> checkpoint, const ui64 unpublishedDataSize)
                : PublicationIntId(publicationIntId)
                , Checkpoint(std::move(checkpoint))
                , UnpublishedDataSize(unpublishedDataSize)
            {}

            const ui64 PublicationIntId = 0;
            const std::optional<NDqProto::TCheckpoint> Checkpoint; // Checkpoint what was associated with data in committing publication (empty when write actor finishing without checkpoints)
            const ui64 UnpublishedDataSize = 0;
            const TInstant StartTime = TInstant::Now();
        };

    public:
        TDeferredPublishState(const i64 currentExecutionGeneration, const ui64 taskId, const ui64 outputIndex, const NPq::NProto::TDqPqTopicSink& sinkParams, const TMetrics& metrics)
            : Metrics(metrics)
            , WriterIdentity(!sinkParams.GetDeferredPublicationExtIdPrefix().empty() ? TStringBuilder() << sinkParams.GetDeferredPublicationExtIdPrefix() << ":" << taskId << ":" << outputIndex << ":" << currentExecutionGeneration : TStringBuilder())
        {}

        operator bool() const {
            return !WriterIdentity.empty();
        }

        bool Empty() const {
            return !InflightPublicationCommit;
        }

        bool NeedToCreatePublication() const {
            return WriterIdentity && !DeferredPublicationIntId;
        }

        ui64 GetCommittingPublicationIntId() const {
            Y_VALIDATE(InflightPublicationCommit, "No committing publication");
            return InflightPublicationCommit->PublicationIntId;
        }

        TString GetSlowPublicationDiagnostics() const {
            TStringBuilder result;
            const auto now = TInstant::Now();
            if (PublicationCreationStartTime && now - PublicationCreationStartTime > SLOW_CHECKPOINT_DURATION) {
                result << "Slow PQ sink publication creation. Duration: " << (now - PublicationCreationStartTime).Seconds() << "s. ";
            }

            if (!InflightPublicationCommit) {
                return result;
            }

            const auto& info = *InflightPublicationCommit;
            const auto duration = now - info.StartTime;
            if (duration <= SLOW_CHECKPOINT_DURATION) {
                return result;
            }

            return result << CheckpointLogString(info.Checkpoint)
                << " Slow PQ sink publication #" << info.PublicationIntId << " commit. Duration: " << duration.Seconds() << 's'
                << ". Checkpoint data size: " << info.UnpublishedDataSize;
        }

        void ResetCurrentPublication() {
            DeferredPublicationIntId = 0;

            if (PublicationStartTime) {
                Metrics.InFlyActivePublications->Dec();
                Metrics.InFlyPendingCommitPublications->Inc();
                Metrics.LastPublicationActiveDuration->Set((TInstant::Now() - PublicationStartTime).MilliSeconds());
                PublicationStartTime = TInstant::Zero();
            }
        }

        void CreatePublication(IDeferredPublishClient& client) {
            Y_VALIDATE(SelfId, "Deferred publish state is not initialized");
            Y_VALIDATE(NeedToCreatePublication(), "Unexpected publication creation");

            if (std::exchange(PublicationCreationStartTime, TInstant::Now())) {
                return;
            }

            NYdb::NTopic::TBeginPublicationSettings settings;
            settings.WriterIdentity(WriterIdentity);
            const auto publicationExtId = TStringBuilder() << WriterIdentity << ":" << PublicationSeqNo++;

            const auto* actorSystem = TActivationContext::ActorSystem();
            client.BeginPublication(publicationExtId, settings).Subscribe([actorSystem, selfId = SelfId, startedAt = PublicationCreationStartTime](const NYdb::NTopic::TAsyncBeginPublicationResult& result) {
                actorSystem->Send(selfId, new TEvPrivate::TEvDeferredPublicationCreated(result.GetValue(), startedAt));
            });
        }

        void OnPublicationCreated(const ui64 publicationIntId) {
            Y_VALIDATE(PublicationCreationStartTime && !DeferredPublicationIntId, "Unexpected publication creation");
            Y_VALIDATE(publicationIntId > 0, "Expected positive publication internal id");
            DeferredPublicationIntId = publicationIntId;
            PublicationStartTime = TInstant::Now();
            PublicationCreationStartTime = TInstant::Zero();
            Metrics.InFlyActivePublications->Inc();
        }

        void CommitPublication(const ui64 publicationIntId, std::optional<NDqProto::TCheckpoint> checkpoint, const ui64 unpublishedDataSize, IDeferredPublishClient& client) {
            Y_VALIDATE(SelfId && WriterIdentity, "Deferred publication are disabled or not initialized");
            Y_VALIDATE(publicationIntId, "Publication is not initialised for checkpoint " << CheckpointLogString(checkpoint));
            Y_VALIDATE(!InflightPublicationCommit, "Parallel publications commit is not supported");

            InflightPublicationCommit.emplace(publicationIntId, std::move(checkpoint), unpublishedDataSize);

            const auto* actorSystem = TActivationContext::ActorSystem();
            client.Publish(NYdb::NTopic::TDeferredPublication(publicationIntId)).Subscribe([actorSystem, selfId = SelfId, startedAt = InflightPublicationCommit->StartTime](const NYdb::NTopic::TAsyncPublishResult& result) {
                actorSystem->Send(selfId, new TEvPrivate::TEvDeferredPublicationCommitted(result.GetValue(), startedAt));
            });
        }

        TInflightPublicationInfo OnPublicationCommitted() {
            Y_VALIDATE(InflightPublicationCommit, "Unexpected committed publication");

            if (const auto dataSize = InflightPublicationCommit->UnpublishedDataSize) {
                // Account only publications what was created for current write session
                Metrics.InFlyPendingCommitPublications->Dec();
                Metrics.UnpublishedDataSize->Sub(dataSize);
            }

            return *std::exchange(InflightPublicationCommit, std::nullopt);
        }

    private:
        const TMetrics& Metrics;
        const TString WriterIdentity;
        YDB_ACCESSOR_DEF(TActorId, SelfId);

        YDB_READONLY(ui64, DeferredPublicationIntId, 0);
        ui64 PublicationSeqNo = 0;
        TInstant PublicationStartTime;
        TInstant PublicationCreationStartTime;
        std::optional<TInflightPublicationInfo> InflightPublicationCommit;
    };

    struct TTopicEventProcessor {
        explicit TTopicEventProcessor(TDqPqWriteActor& self)
            : Self(self)
        {}

        bool operator()(const NYdb::NTopic::TSessionClosedEvent& ev) {
            Self.Fail(ev, TStringBuilder() << "Write session to topic \"" << Self.SinkParams.GetTopicPath() << "\" was closed");
            return false;
        }

        bool operator()(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            SINK_LOG_T("Received continuation token, buffer size: " << Self.Buffer.GetMessages().size());
            Self.Metrics.ReportFirstContinuationToken();
            Self.ContinuationToken = std::move(ev.ContinuationToken);
            return true;
        }

        bool operator()(const NYdb::NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            const auto& acks = ev.Acks;
            SINK_LOG_T("Got acks #" << acks.size());

            for (const auto& sdkAck : ev.Acks) {
                const auto sdkAckSeqNo = sdkAck.SeqNo;
                const auto state = sdkAck.State;
                SINK_LOG_T("Ack seq no (from TAcksEvent) " << sdkAckSeqNo << ", state: " << state);

                if (state == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_DISCARDED) {
                    Self.Fail(TStringBuilder() << "Message with seqNo " << sdkAckSeqNo << " was discarded");
                    return false;
                }

                if (state == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_ALREADY_WRITTEN) {
                    Self.Metrics.AlreadyWritten->Inc();
                }

                Y_VALIDATE(!Self.Buffer.GetInflight().empty(), "Got unexpected ack with seq no: " << sdkAckSeqNo);
                const auto& ackInfo = Self.Buffer.PopAck();
                Self.FreeSpace += ackInfo.MessageSize;
                SINK_LOG_T("Ack seq no (from inflight ack) " << ackInfo.SeqNo << ", message size: " << ackInfo.MessageSize << ", free space: " << Self.FreeSpace);
            }

            return true;
        }

    private:
        TString LogPrefix() const {
            return TStringBuilder() << Self.LogPrefix() << "[TTopicEventProcessor] ";
        }

        TDqPqWriteActor& Self;
    };

public:
    static constexpr char ActorName[] = "DQ_PQ_WRITE_ACTOR";

    TDqPqWriteActor(
        const ui64 outputIndex, const TCollectStatsLevel statsLevel, TTxId txId, const ui64 taskId, NPq::NProto::TDqPqTopicSink&& sinkParams,
        NYdb::TDriver driver, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory, IDqComputeActorAsyncOutput::ICallbacks* const callbacks,
        ::NMonitoring::TDynamicCounterPtr counters, const i64 freeSpace, const i64 currentExecutionGeneration, IPqStaticGateway::TPtr pqGateway,
        const bool enableStreamingQueriesCounters, const bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag, const bool hasCheckpoints
    )
        : TActor<TDqPqWriteActor>(&TDqPqWriteActor::StateFunc)
        , OutputIndex(outputIndex)
        , TaskId(taskId)
        , TxId(std::move(txId))
        , SinkParams(std::move(sinkParams))
        , Callbacks(callbacks)
        , PqGateway(std::move(pqGateway))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , Driver(std::move(driver))
        , Metrics(TxId, TaskId, std::move(counters), enableStreamingQueriesCounters, /* enableCountersPerTask */ false, !SinkParams.GetDeferredPublicationExtIdPrefix().empty())
        , EnableDeduplication(enableStreamingQueriesPqSinkDeduplicationFeatureFlag && SinkParams.GetEnableDeduplication())
        , HasCheckpoints(hasCheckpoints)
        , FreeSpace(freeSpace)
        , Buffer(EgressStats, Metrics, EnableDeduplication)
        , CheckpointsState(EgressStats, Metrics)
        , DeferredPublishState(currentExecutionGeneration, TaskId, OutputIndex, SinkParams, Metrics)
    {
        Y_VALIDATE(Callbacks, "Missing callbacks");
        Y_VALIDATE(!EnableDeduplication || !DeferredPublishState, "Deferred publications can not be used with deduplication");

        EgressStats.Level = statsLevel;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvExecuteTopicEvent, HandleTopicEvent);
        hFunc(TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(TEvPrivate::TEvDeferredPublicationCreated, Handle);
        hFunc(TEvPrivate::TEvDeferredPublicationCommitted, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
    )

private:
    // IActor

    void Registered(TActorSystem* sys, const TActorId& owner) final {
        TBase::Registered(sys, owner);
        OwnerId = owner;
        CheckpointsState.SetSelfId(SelfId());
        DeferredPublishState.SetSelfId(SelfId());
    }

    void PassAway() final { // Is called from Compute Actor, implements IActor & IDqComputeActorAsyncOutput
        if (WriteSession) {
            WriteSession->Close(/* closeTimeout */ TDuration::Zero());
        }

        TBase::PassAway();
    }

    // IActorExceptionHandler

    bool OnUnhandledException(const std::exception& e) final {
        Fail(NDqProto::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected exception: " << e.what());
        return true;
    }

    // IDqComputeActorAsyncOutput

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    i64 GetFreeSpace() const final {
        return FreeSpace;
    }

    const TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    void SendData(TUnboxedValueBatch&& batch, const i64 dataSize, const TMaybe<NDqProto::TCheckpoint>& checkpoint, const bool finished) final {
        SINK_LOG_T("SendData. Batch: " << batch.RowCount() << ". Has checkpoint: " << checkpoint.Defined() << ". Finished: " << finished);
        Y_UNUSED(dataSize);

        if (finished) {
            Finished = true;
        }

        const auto initialFreeSpace = FreeSpace;

        Y_VALIDATE(!batch.IsWide(), "Wide batch is not supported");
        if (!batch.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            if (!value.IsBoxed()) {
                Fail("Struct with single field was expected");
                return false;
            }

            const NUdf::TUnboxedValue dataCol = value.GetElement(0);
            if (!dataCol.IsString() && !dataCol.IsEmbedded()) {
                Fail("Non string value could not be written to YDS stream");
                return false;
            }

            TString data(dataCol.AsStringRef());

            LWPROBE(PqWriteDataToSend, ToString(TxId), SinkParams.GetTopicPath(), data);
            SINK_LOG_T("Received data for sending: " << data);

            const auto messageSize = GetItemSize(data);
            if (messageSize > MAX_MESSAGE_SIZE) {
                Fail(TStringBuilder() << "Max message size for YDS is " << MAX_MESSAGE_SIZE
                    << " bytes but received message with size of " << messageSize << " bytes");
                return false;
            }

            FreeSpace -= messageSize;
            Buffer.PushMessage(std::move(data));
            return true;
        })) {
            return;
        }

        if (checkpoint) {
            const auto lastBufferedSeqNo = Buffer.GetLastBufferedSeqNo();
            SINK_LOG_D(CheckpointLogString(*checkpoint) << " Register checkpoint for seq no: " << lastBufferedSeqNo);
            CheckpointsState.PushCheckpoint(lastBufferedSeqNo, *checkpoint);
        }

        if (initialFreeSpace > 0 && FreeSpace <= 0) {
            SINK_LOG_D("Sink paused by free space: " << initialFreeSpace << " -> " << FreeSpace);
        }

        Process();
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) final {
        SINK_LOG_D("Commit state: " << CheckpointLogString(checkpoint));

        if (const auto& info = CheckpointsState.CommitCheckpoints(checkpoint); info && info->DeferredPublicationIntId) {
            SINK_LOG_D("Commit deferred publication " << info->DeferredPublicationIntId << " on checkpoint " << CheckpointLogString(checkpoint));
            DeferredPublishState.CommitPublication(info->DeferredPublicationIntId, checkpoint, info->DataSizeUnderCheckpoint, GetDeferredPublishClient());
        } else {
            SINK_LOG_D("Commit checkpoint immediately " << CheckpointLogString(checkpoint));
            Callbacks->OnAsyncOutputStateCommitted(OutputIndex, checkpoint);
        }

        Process();
    }

    void LoadState(const TSinkState& state, const NDqProto::TCheckpoint& checkpoint) final {
        const auto& data = state.Data;
        Y_VALIDATE(data.Version == STATE_VERSION, "Invalid state version " << data.Version);

        NPq::NProto::TDqPqTopicSinkState stateProto;
        YQL_ENSURE(stateProto.ParseFromString(data.Blob), "Serialized state is corrupted");
        SINK_LOG_D("Load state: " << stateProto);

        SourceId = stateProto.GetSourceId();
        EgressStats.Bytes = stateProto.GetEgressBytes();
        CheckpointsState.SetWrittenDataUnderCheckpoint(EgressStats.Bytes);

        const auto confirmedSeqNo = stateProto.GetConfirmedSeqNo();
        Buffer.LoadConfirmedSeqNo(confirmedSeqNo);

        // Register checkpoint for committing in case of restoring from partially saved checkpoint
        CheckpointsState.LoadPendingCommitCheckpoint(confirmedSeqNo, checkpoint, stateProto.GetDeferredPublicationIntId());
    }

    // Events

    void Handle(TEvPrivate::TEvPqEventsReady::TPtr&) {
        SINK_LOG_T("New PQ write session events arrived");
        Process();
        SubscribeOnNextEvent();
    }

    void Handle(TEvPrivate::TEvDeferredPublicationCreated::TPtr& ev) {
        Metrics.LastBeginPublicationLatency->Set(ev->Get()->Latency.MilliSeconds());

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            Fail(result, "Failed to create deferred publication");
            return;
        }

        const auto intPublicationId = result.GetIntPublicationId();
        SINK_LOG_D("Publication created, internal id: " << intPublicationId << ", external id: " << result.GetPublication().ExtPublicationId.value_or("<null>"));
        DeferredPublishState.OnPublicationCreated(intPublicationId);

        Process();
    }

    void Handle(TEvPrivate::TEvDeferredPublicationCommitted::TPtr& ev) {
        Metrics.LastPublishLatency->Set(ev->Get()->Latency.MilliSeconds());

        const auto publicationIntId = DeferredPublishState.GetCommittingPublicationIntId();
        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::NOT_FOUND) {
            Fail(result, TStringBuilder() << "Failed to commit deferred publication #" << publicationIntId);
            return;
        }

        if (result.IsSuccess()) {
            SINK_LOG_D("Publication #" << publicationIntId << " committed");
        } else {
            SINK_LOG_I("Publication #" << publicationIntId << " not found, considering as committed");
        }

        if (const auto& info = DeferredPublishState.OnPublicationCommitted(); info.Checkpoint) {
            Callbacks->OnAsyncOutputStateCommitted(OutputIndex, *info.Checkpoint);
        }

        Process();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        if (const auto& diagnostics = CheckpointsState.GetSlowCheckpointDiagnostics(Buffer.GetLastSentMessageSeqNo(), Buffer.GetConfirmedSeqNo())) {
            SINK_LOG_W(diagnostics);
        }

        if (const auto& diagnostics = DeferredPublishState.GetSlowPublicationDiagnostics()) {
            SINK_LOG_W(diagnostics);
        }
    }

    // Initialization

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() { // Must be called after actor registration
        Y_VALIDATE(OwnerId, "Can not create federated topic client settings before actor registration");
        NYdb::NFederatedTopic::TFederatedTopicClientSettings opts = PqGateway->GetFederatedTopicClientSettings();

        if (SinkParams.GetUseActorSystemThreadsInTopicClient()) {
            SetupTopicClientSettings(ActorContext().ActorSystem(), SelfId(), opts);
        }

        opts.Database(SinkParams.GetDatabase())
            .DiscoveryEndpoint(SinkParams.GetEndpoint())
            .SslCredentials(NYdb::TSslCredentials(SinkParams.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsProviderFactory);

        return opts;
    }

    IFederatedTopicClient& GetFederatedTopicClient() {
        if (!FederatedTopicClient) {
            FederatedTopicClient = PqGateway->GetFederatedTopicClient(Driver, GetFederatedTopicClientSettings());
            Y_VALIDATE(FederatedTopicClient, "Failed to create federated topic client");
        }

        return *FederatedTopicClient;
    }

    IDeferredPublishClient& GetDeferredPublishClient() {
        if (!DeferredPublishClient) {
            DeferredPublishClient = PqGateway->GetDeferredPublishClient(Driver, NYdb::TCommonClientSettings()
                .Database(SinkParams.GetDatabase())
                .DiscoveryEndpoint(SinkParams.GetEndpoint())
                .SslCredentials(NYdb::TSslCredentials(SinkParams.GetUseSsl()))
                .CredentialsProviderFactory(CredentialsProviderFactory)
            );
            Y_VALIDATE(DeferredPublishClient, "Failed to create deferred publish client");
        }

        return *DeferredPublishClient;
    }

    const TString& GetSourceId() { // Must be called after state loading
        if (!SourceId) {
            SourceId = CreateGuidAsString(); // Not loaded from state, so this is the first run.
            SINK_LOG_D("Created new source id: " << SourceId);
        }

        return SourceId;
    }

    NYdb::NTopic::TWriteSessionSettings GetWriteSessionSettings() {
        auto settings = NYdb::NTopic::TWriteSessionSettings()
            .Path(SinkParams.GetTopicPath())
            .TraceId(LogPrefix())
            .MaxMemoryUsage(FreeSpace)
            .DeduplicationEnabled(EnableDeduplication)
            .Codec(SinkParams.GetClusterType() == NPq::NProto::DataStreams
                ? NYdb::NTopic::ECodec::RAW
                : NYdb::NTopic::ECodec::GZIP);

        if (EnableDeduplication) {
            const auto& sourceId = GetSourceId();
            settings.ProducerId(sourceId);
            settings.MessageGroupId(sourceId);
        }

        return settings;
    }

    void CreateSessionIfNotExists() {
        if (!WriteSession) {
            SINK_LOG_D("Create new PQ write session");
            WriteSession = GetFederatedTopicClient().CreateWriteSession(GetWriteSessionSettings());
            SubscribeOnNextEvent();
        }
    }

    // Message processing

    void Process() {
        if (Failed) {
            // Skip iteration, fail status was already sent to compute actor
            return;
        }

        CreateSessionIfNotExists();

        for (bool canSend = true; canSend;) {
            if (!HandleNewPQEvents()) {
                // Write session returned error
                return;
            }

            canSend = !Buffer.GetMessages().empty() && ContinuationToken && !DeferredPublishState.NeedToCreatePublication();

            if (canSend) {
                WriteNextMessage(std::move(*ContinuationToken));
                ContinuationToken.reset();
            }

            if (CheckpointsState.AdvanceSentSeqNo(Buffer.GetLastSentMessageSeqNo(), DeferredPublishState.GetDeferredPublicationIntId())) {
                DeferredPublishState.ResetCurrentPublication();
            }
        }

        if (const auto& info = CheckpointsState.AdvanceConfirmedSeqNo(Buffer.GetConfirmedSeqNo())) {
            SINK_LOG_D(CheckpointLogString(info->Checkpoint) << " Save state for deferred checkpoint with seq no: " << info->WaitConfirmedSeqNo);
            Callbacks->OnAsyncOutputStateSaved(BuildState(*info), OutputIndex, info->Checkpoint);
        }

        if (!Buffer.GetMessages().empty() && DeferredPublishState.NeedToCreatePublication()) {
            SINK_LOG_D("Creating new deferred publication");
            DeferredPublishState.CreatePublication(GetDeferredPublishClient());
        }

        CheckFinished();
    }

    void SubscribeOnNextEvent() const {
        if (!WriteSession) {
            return;
        }

        SINK_LOG_T("Subscribe on next event");
        const auto* actorSystem = TActivationContext::ActorSystem();
        WriteSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&) {
            actorSystem->Send(selfId, new TEvPrivate::TEvPqEventsReady());
        });
    }

    bool HandleNewPQEvents() {
        if (!WriteSession) {
            return false;
        }

        auto events = WriteSession->GetEvents();
        const auto initialFreeSpace = FreeSpace;
        SINK_LOG_T("Extracted #" << events.size() << " PQ write session events, free space: " << initialFreeSpace);

        for (auto& event : events) {
            if (!std::visit(TTopicEventProcessor{*this}, event)) {
                return false;
            }
        }

        if (initialFreeSpace <= 0 && FreeSpace > 0) {
            SINK_LOG_D("Sink resumed by free space: " << initialFreeSpace << " -> " << FreeSpace);
            Callbacks->ResumeExecution();
        }

        return true;
    }

    void WriteNextMessage(NYdb::NTopic::TContinuationToken&& token) {
        if (!WriteSession) {
            return;
        }

        const auto& [seqNo, messageData] = Buffer.PopMessage();
        SINK_LOG_T("Write message into PQ session: " << messageData);

        NYdb::NTopic::TWriteMessage message(messageData);
        message.SeqNo(seqNo);

        if (const auto deferredPublicationIntId = DeferredPublishState.GetDeferredPublicationIntId()) {
            message.DeferredPublication(NYdb::NTopic::TDeferredPublication(deferredPublicationIntId));
            Metrics.UnpublishedDataSize->Add(GetItemSize(messageData));
        } else {
            Y_VALIDATE(!DeferredPublishState, "Unexpected deferred publication state");
        }

        WriteSession->Write(std::move(token), std::move(message));
    }

    TSinkState BuildState(const TCheckpointsState::TCheckpointInfo& info) {
        NPq::NProto::TDqPqTopicSinkState stateProto;
        stateProto.SetSourceId(GetSourceId());
        stateProto.SetConfirmedSeqNo(info.WaitConfirmedSeqNo);
        stateProto.SetEgressBytes(info.EgressBytes);
        stateProto.SetDeferredPublicationIntId(info.DeferredPublicationIntId);

        TSinkState sinkState;
        auto& data = sinkState.Data;
        data.Version = STATE_VERSION;
        YQL_ENSURE(stateProto.SerializeToString(&data.Blob));
        SINK_LOG_T("Save checkpoint " << info.Checkpoint << " state: " << stateProto);

        return sinkState;
    }

    void CheckFinished() {
        if (!Finished || !Buffer.Empty() || !CheckpointsState.Empty() || !DeferredPublishState.Empty()) {
            return;
        }

        if (const auto publicationIntId = DeferredPublishState.GetDeferredPublicationIntId()) {
            if (HasCheckpoints) {
                SINK_LOG_T("Waiting for one checkpoint after last write");
                return;
            }

            // In case of disabled checkpoints, publications should be committed once on finish
            SINK_LOG_I("Commit publication " << publicationIntId << " after finish without checkpoints");
            DeferredPublishState.ResetCurrentPublication();
            DeferredPublishState.CommitPublication(publicationIntId, /* checkpoint */ std::nullopt, EgressStats.Bytes - CheckpointsState.GetWrittenDataUnderCheckpoint(), GetDeferredPublishClient());
            return;
        }

        SINK_LOG_D("Notify PQ sink finished");
        Callbacks->OnAsyncOutputFinished(OutputIndex);
    }

    // Common

    void Fail(const NDqProto::StatusIds::StatusCode status, const TIssues& issues) {
        Failed = true;

        if (WriteSession) {
            WriteSession->Close(TDuration::Zero());
            WriteSession.reset();
        }

        Y_VALIDATE(!IsIn({NDqProto::StatusIds::SUCCESS, NDqProto::StatusIds::UNSPECIFIED}, status), "Invalid fail status: " << status);
        SINK_LOG_W("Fail. Status: " << NDqProto::StatusIds::StatusCode_Name(status) << ". Issues: " << issues.ToOneLineString());
        Callbacks->OnAsyncOutputError(OutputIndex, issues, status);
    }

    void Fail(const NDqProto::StatusIds::StatusCode status, const TString& message) {
        Fail(status, {TIssue(message)});
    }

    void Fail(const TString& message) {
        Fail(NDqProto::StatusIds::EXTERNAL_ERROR, message);
    }

    void Fail(const NYdb::TStatus& status, const TString& message) {
        Y_VALIDATE(!status.IsSuccess(), "Unexpected success status for fail");

        TIssue rootIssue(TStringBuilder() << message << ". Status: " << status.GetStatus());
        for (const auto& issue : status.GetIssues()) {
            rootIssue.AddSubIssue(MakeIntrusive<TIssue>(NYdb::NAdapters::ToYqlIssue(issue)));
        }

        Fail(NDqProto::StatusIds::EXTERNAL_ERROR, {rootIssue});
    }

    TString LogPrefix() const {
        auto prefix = TStringBuilder() << "[" << ActorName << "] ";

        if (OwnerId) {
            prefix << "OwnerId: " << *OwnerId << ". ActorId: " << SelfId() << ". ";
        }

        return prefix << "TxId: " << TxId << ". TaskId: " << TaskId << ". OutputIndex: " << OutputIndex << ". ";
    }

    static i64 GetItemSize(const TString& item) {
        return std::max(static_cast<i64>(item.size()), static_cast<i64>(1));
    }

    static TString CheckpointLogString(const NDqProto::TCheckpoint& checkpoint) {
        return TStringBuilder() << "[Checkpoint " << checkpoint.GetGeneration() << "." << checkpoint.GetId() << "]";
    }

    static TString CheckpointLogString(const std::optional<NDqProto::TCheckpoint>& checkpoint) {
        return checkpoint ? CheckpointLogString(*checkpoint) : "[Checkpoint <null>]";
    }

    const ui64 OutputIndex = 0;
    const ui64 TaskId;
    const TTxId TxId;
    const NPq::NProto::TDqPqTopicSink SinkParams;
    IDqComputeActorAsyncOutput::ICallbacks* const Callbacks = nullptr;
    const IPqStaticGateway::TPtr PqGateway;
    const NYdb::TCredentialsProviderFactoryPtr CredentialsProviderFactory;
    const NYdb::TDriver Driver;
    const TMetrics Metrics;
    const bool EnableDeduplication = false;
    const bool HasCheckpoints = false;

    std::optional<TActorId> OwnerId;
    TDqAsyncStats EgressStats;
    TString SourceId;
    IFederatedTopicClient::TPtr FederatedTopicClient;
    IDeferredPublishClient::TPtr DeferredPublishClient;
    std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;

    i64 FreeSpace = 0;
    bool Finished = false;
    bool Failed = false;
    TDataBuffer Buffer;
    TCheckpointsState CheckpointsState;
    TDeferredPublishState DeferredPublishState;
    std::optional<NYdb::NTopic::TContinuationToken> ContinuationToken;
};

} // anonymous namespace

std::pair<IDqComputeActorAsyncOutput*, IActor*> CreateDqPqWriteActor(
    NPq::NProto::TDqPqTopicSink&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    IPqStaticGateway::TPtr pqGateway,
    bool enableStreamingQueriesCounters,
    i64 freeSpace,
    i64 currentExecutionGeneration,
    bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag,
    bool hasCheckpoints)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqWriteActor* actor = new TDqPqWriteActor(
        outputIndex,
        statsLevel,
        txId,
        taskId,
        std::move(settings),
        std::move(driver),
        credentialsFactory->Create(token, addBearerToToken),
        callbacks,
        counters,
        freeSpace,
        currentExecutionGeneration,
        pqGateway,
        enableStreamingQueriesCounters,
        enableStreamingQueriesPqSinkDeduplicationFeatureFlag,
        hasCheckpoints
    );
    return {actor, actor};
}

void RegisterDqPqWriteActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory, const IPqStaticGateway::TPtr& pqGateway, const ::NMonitoring::TDynamicCounterPtr& counters, bool enableStreamingQueriesCounters, bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag) {
    factory.RegisterSink<NPq::NProto::TDqPqTopicSink>("PqSink",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters, pqGateway, enableStreamingQueriesCounters, enableStreamingQueriesPqSinkDeduplicationFeatureFlag](
            NPq::NProto::TDqPqTopicSink&& settings,
            IDqAsyncIoFactory::TSinkArguments&& args)
        {
            auto txId = args.TxId;
            if (const auto it = args.TaskParams.find("query_path"); it != args.TaskParams.end()) {
                txId = it->second;
            }

            i64 currentExecutionGeneration = 0;
            if (const auto it = args.TaskParams.find("current_execution_generation"); it != args.TaskParams.end()) {
                currentExecutionGeneration = FromString<i64>(it->second);
            }

            bool hasCheckpoints = false;
            if (const auto it = args.TaskParams.find("checkpoints_enabled"); it != args.TaskParams.end()) {
                hasCheckpoints = FromString<bool>(it->second) && args.HasCheckpoints;
            }

            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
            return CreateDqPqWriteActor(
                std::move(settings),
                args.OutputIndex,
                args.StatsLevel,
                txId,
                args.TaskId,
                args.SecureParams,
                driver,
                credentialsFactory,
                args.Callback,
                counters ? counters : args.TaskCounters,
                pqGateway,
                enableStreamingQueriesCounters,
                DqPqDefaultFreeSpace,
                currentExecutionGeneration,
                enableStreamingQueriesPqSinkDeduplicationFeatureFlag,
                hasCheckpoints
            );
        }
    );
}

} // namespace NYql::NDq
