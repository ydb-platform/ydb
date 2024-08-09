#include "dq_pq_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

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

constexpr ui32 StateVersion = 1;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvSourceDataReady = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct TEvSourceDataReady : public TEventLocal<TEvSourceDataReady, EvSourceDataReady> {};
};

} // namespace

class TDqPqReadActor : public NActors::TActor<TDqPqReadActor>, public IDqComputeActorAsyncInput {
    struct TMetrics {
        TMetrics(const TTxId& txId, ui64 taskId, const ::NMonitoring::TDynamicCounterPtr& counters)
            : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
            , Counters(counters) {
            SubGroup = Counters->GetSubgroup("sink", "PqRead");
            auto sink = SubGroup->GetSubgroup("tx_id", TxId);
            auto task = sink->GetSubgroup("task_id", ToString(taskId));
            InFlyAsyncInputData = task->GetCounter("InFlyAsyncInputData");
            InFlySubscribe = task->GetCounter("InFlySubscribe");
            AsyncInputDataRate = task->GetCounter("AsyncInputDataRate", true);
        }

        ~TMetrics() {
            SubGroup->RemoveSubgroup("id", TxId);
        }

        TString TxId;
        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounterPtr SubGroup;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
        ::NMonitoring::TDynamicCounters::TCounterPtr AsyncInputDataRate;
    };

public:
    using TPartitionKey = std::pair<TString, ui64>; // Cluster, partition id.
    using TDebugOffsets = TMaybe<std::pair<ui64, ui64>>;

    TDqPqReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize)
        : TActor<TDqPqReadActor>(&TDqPqReadActor::StateFunc)
        , InputIndex(inputIndex)
        , TxId(txId)
        , Metrics(txId, taskId, counters)
        , BufferSize(bufferSize)
        , HolderFactory(holderFactory)
        , LogPrefix(TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << taskId << ". PQ source. ")
        , Driver(std::move(driver))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , SourceParams(std::move(sourceParams))
        , ReadParams(std::move(readParams))
        , StartingMessageTimestamp(TInstant::MilliSeconds(TInstant::Now().MilliSeconds())) // this field is serialized as milliseconds, so drop microseconds part to be consistent with storage
        , ComputeActorId(computeActorId)
    {
        MetadataFields.reserve(SourceParams.MetadataFieldsSize());
        TPqMetaExtractor fieldsExtractor;
        for (const auto& fieldName : SourceParams.GetMetadataFields()) {
            MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
        }

        InitWatermarkTracker();
        IngressStats.Level = statsLevel;
    }

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const {
        NYdb::NTopic::TTopicClientSettings opts;
        opts.Database(SourceParams.GetDatabase())
            .DiscoveryEndpoint(SourceParams.GetEndpoint())
            .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsProviderFactory);

        return opts;
    }

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

public:
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
        NPq::NProto::TDqPqTopicSourceState stateProto;

        NPq::NProto::TDqPqTopicSourceState::TTopicDescription* topic = stateProto.AddTopics();
        topic->SetDatabaseId(SourceParams.GetDatabaseId());
        topic->SetEndpoint(SourceParams.GetEndpoint());
        topic->SetDatabase(SourceParams.GetDatabase());
        topic->SetTopicPath(SourceParams.GetTopicPath());

        for (const auto& [clusterAndPartition, offset] : PartitionToOffset) {
            const auto& [cluster, partition] = clusterAndPartition;
            NPq::NProto::TDqPqTopicSourceState::TPartitionReadState* partitionState = stateProto.AddPartitions();
            partitionState->SetTopicIndex(0); // Now we are supporting only one topic per source.
            partitionState->SetCluster(cluster);
            partitionState->SetPartition(partition);
            partitionState->SetOffset(offset);
        }

        stateProto.SetStartingMessageTimestampMs(StartingMessageTimestamp.MilliSeconds());
        stateProto.SetIngressBytes(IngressStats.Bytes);

        TString stateBlob;
        YQL_ENSURE(stateProto.SerializeToString(&stateBlob));

        state.Data.emplace_back(stateBlob, StateVersion);

        DeferredCommits.emplace(checkpoint.GetId(), std::move(CurrentDeferredCommit));
        CurrentDeferredCommit = NYdb::NTopic::TDeferredCommit();
    }

    void LoadState(const TSourceState& state) override {
        TInstant minStartingMessageTs = state.DataSize() ? TInstant::Max() : StartingMessageTimestamp;
        ui64 ingressBytes = 0;
        for (const auto& data : state.Data) {
            if (data.Version == StateVersion) { // Current version
                NPq::NProto::TDqPqTopicSourceState stateProto;
                YQL_ENSURE(stateProto.ParseFromString(data.Blob), "Serialized state is corrupted");
                YQL_ENSURE(stateProto.TopicsSize() == 1, "One topic per source is expected");
                PartitionToOffset.reserve(PartitionToOffset.size() + stateProto.PartitionsSize());
                for (const NPq::NProto::TDqPqTopicSourceState::TPartitionReadState& partitionProto : stateProto.GetPartitions()) {
                    ui64& offset = PartitionToOffset[TPartitionKey{partitionProto.GetCluster(), partitionProto.GetPartition()}];
                    if (offset) {
                        offset = Min(offset, partitionProto.GetOffset());
                    } else {
                        offset = partitionProto.GetOffset();
                    }
                }
                minStartingMessageTs = Min(minStartingMessageTs, TInstant::MilliSeconds(stateProto.GetStartingMessageTimestampMs()));
                ingressBytes += stateProto.GetIngressBytes();
            } else {
                ythrow yexception() << "Invalid state version " << data.Version;
            }
        }
        for (const auto& [key, value] : PartitionToOffset) {
            SRC_LOG_D("SessionId: " << GetSessionId() << " Restoring offset: cluster " << key.first << ", partition id " << key.second << ", offset: " << value);
        }
        StartingMessageTimestamp = minStartingMessageTs;
        IngressStats.Bytes += ingressBytes;
        IngressStats.Chunks++;
        InitWatermarkTracker();

        if (ReadSession) {
            ReadSession.reset();
            GetReadSession();
        }
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        const auto checkpointId = checkpoint.GetId();
        while (!DeferredCommits.empty() && DeferredCommits.front().first <= checkpointId) {
            auto& deferredCommit = DeferredCommits.front().second;
            deferredCommit.Commit();
            DeferredCommits.pop();
        }
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    NYdb::NTopic::TTopicClient& GetTopicClient() {
        if (!TopicClient) {
            TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings());
        }
        return *TopicClient;
    }

    NYdb::NTopic::IReadSession& GetReadSession() {
        if (!ReadSession) {
            ReadSession = GetTopicClient().CreateReadSession(GetReadSessionSettings());
            SRC_LOG_I("SessionId: " << GetSessionId() << " CreateReadSession");
        }
        return *ReadSession;
    }

    TString GetSessionId() const {
        return ReadSession ? ReadSession->GetSessionId() : TString{"empty"};
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSourceDataReady, Handle);
    )

    void Handle(TEvPrivate::TEvSourceDataReady::TPtr& ev) {
        SRC_LOG_T("SessionId: " << GetSessionId() << " Source data ready");
        SubscribedOnEvent = false;
        if (ev.Get()->Cookie) {
            Metrics.InFlySubscribe->Dec();
        }
        Metrics.InFlyAsyncInputData->Set(1);
        Metrics.AsyncInputDataRate->Inc();
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        std::queue<TReadyBatch> empty;
        ReadyBuffer.swap(empty);

        if (ReadSession) {
            ReadSession->Close(TDuration::Zero());
            ReadSession.reset();
        }
        TopicClient.reset();
        TActor<TDqPqReadActor>::PassAway();
    }

    void MaybeScheduleNextIdleCheck(TInstant systemTime) {
        if (!WatermarkTracker) {
            return;
        }

        const auto nextIdleCheckAt = WatermarkTracker->GetNextIdlenessCheckAt(systemTime);
        if (!nextIdleCheckAt) {
            return;
        }

        if (!NextIdlenesCheckAt.Defined() || nextIdleCheckAt != *NextIdlenesCheckAt) {
            NextIdlenesCheckAt = *nextIdleCheckAt;
            SRC_LOG_T("SessionId: " << GetSessionId() << " Next idleness check scheduled at " << *nextIdleCheckAt);
            Schedule(*nextIdleCheckAt, new TEvPrivate::TEvSourceDataReady());
        }
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override {
        Metrics.InFlyAsyncInputData->Set(0);
        SRC_LOG_T("SessionId: " << GetSessionId() << " GetAsyncInputData freeSpace = " << freeSpace);

        const auto now = TInstant::Now();
        MaybeScheduleNextIdleCheck(now);

        i64 usedSpace = 0;
        if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
            return usedSpace;
        }

        bool recheckBatch = false;

        if (freeSpace > 0) {
            auto events = GetReadSession().GetEvents(false, TMaybe<size_t>(), static_cast<size_t>(freeSpace));
            recheckBatch = !events.empty();

            ui32 batchItemsEstimatedCount = 0;
            for (auto& event : events) {
                if (const auto* val = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    batchItemsEstimatedCount += val->GetMessages().size();
                }
            }
            for (auto& event : events) {
                std::visit(TTopicEventProcessor{*this, batchItemsEstimatedCount, LogPrefix}, event);
            }
        }

        if (WatermarkTracker) {
            const auto watermark = WatermarkTracker->HandleIdleness(now);

            if (watermark) {
                const auto t = watermark;
                SRC_LOG_T("SessionId: " << GetSessionId() << " Fake watermark " << t << " was produced");
                PushWatermarkToReady(*watermark);
                recheckBatch = true;
            }
        }

        if (recheckBatch) {
            usedSpace = 0;
            if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
                return usedSpace;
            }
        }

        watermark = Nothing();
        buffer.clear();
        return 0;
    }

private:
    std::vector<ui64> GetPartitionsToRead() const {
        std::vector<ui64> res;

        ui64 currentPartition = ReadParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
        do {
            res.emplace_back(currentPartition); // 0-based in topic API
            currentPartition += ReadParams.GetPartitioningParams().GetDqPartitionsCount();
        } while (currentPartition < ReadParams.GetPartitioningParams().GetTopicPartitionsCount());

        return res;
    }

    void InitWatermarkTracker() {
        SRC_LOG_D("SessionId: " << GetSessionId() << " Watermarks enabled: " << SourceParams.GetWatermarks().GetEnabled() << " granularity: "
            << SourceParams.GetWatermarks().GetGranularityUs() << " microseconds");

        if (!SourceParams.GetWatermarks().GetEnabled()) {
            return;
        }

        WatermarkTracker.ConstructInPlace(
            TDuration::MicroSeconds(SourceParams.GetWatermarks().GetGranularityUs()),
            StartingMessageTimestamp,
            SourceParams.GetWatermarks().GetIdlePartitionsEnabled(),
            TDuration::MicroSeconds(SourceParams.GetWatermarks().GetLateArrivalDelayUs()),
            TInstant::Now());
    }

    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings() const {
        NYdb::NTopic::TTopicReadSettings topicReadSettings;
        topicReadSettings.Path(SourceParams.GetTopicPath());
        auto partitionsToRead = GetPartitionsToRead();
        SRC_LOG_D("SessionId: " << GetSessionId() << " PartitionsToRead: " << JoinSeq(", ", partitionsToRead));
        for (const auto partitionId : partitionsToRead) {
            topicReadSettings.AppendPartitionIds(partitionId);
        }

        return NYdb::NTopic::TReadSessionSettings()
            .AppendTopics(topicReadSettings)
            .ConsumerName(SourceParams.GetConsumerName())
            .MaxMemoryUsageBytes(BufferSize)
            .ReadFromTimestamp(StartingMessageTimestamp);
    }

    static TPartitionKey MakePartitionKey(const NYdb::NTopic::TPartitionSession::TPtr& partitionSession) {
        // auto cluster = partitionSession->GetDatabaseName() // todo: switch to federatedfTopicApi to support lb federation
        const TString cluster; // empty value is used in YDS
        return std::make_pair(cluster, partitionSession->GetPartitionId());
    }

    void SubscribeOnNextEvent() {
        if (!SubscribedOnEvent) {
            SubscribedOnEvent = true;
            Metrics.InFlySubscribe->Inc();
            NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
            EventFuture = GetReadSession().WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
                actorSystem->Send(selfId, new TEvPrivate::TEvSourceDataReady(), 0, 1);
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
        THashMap<NYdb::NTopic::TPartitionSession::TPtr, TList<std::pair<ui64, ui64>>> OffsetRanges; // [start, end)
    };

    bool MaybeReturnReadyBatch(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, i64& usedSpace) {
        if (ReadyBuffer.empty()) {
            SubscribeOnNextEvent();
            return false;
        }

        auto& readyBatch = ReadyBuffer.front();
        buffer.clear();
        std::move(readyBatch.Data.begin(), readyBatch.Data.end(), std::back_inserter(buffer));
        watermark = readyBatch.Watermark;
        usedSpace = readyBatch.UsedSpace;

        for (const auto& [PartitionSession, ranges] : readyBatch.OffsetRanges) {
            for (const auto& [start, end] : ranges) {
                CurrentDeferredCommit.Add(PartitionSession, start, end);
            }
            PartitionToOffset[MakePartitionKey(PartitionSession)] = ranges.back().second;
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

    void PushWatermarkToReady(TInstant watermark) {
        SRC_LOG_D("SessionId: " << GetSessionId() << " New watermark " << watermark << " was generated");

        if (Y_UNLIKELY(ReadyBuffer.empty() || ReadyBuffer.back().Watermark.Defined())) {
            ReadyBuffer.emplace(watermark, 0);
            return;
        }

        ReadyBuffer.back().Watermark = watermark;
    }

    struct TTopicEventProcessor {
        static TString ToString(const TPartitionKey& key) {
            return TStringBuilder{} << "[" << key.first << ", " << key.second << "]";
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
            const auto partitionKeyStr = ToString(partitionKey);
            for (const auto& message : event.GetMessages()) {
                const TString& data = message.GetData();
                Self.IngressStats.Bytes += data.size();
                LWPROBE(PqReadDataReceived, TString(TStringBuilder() << Self.TxId), Self.SourceParams.GetTopicPath(), data);
                SRC_LOG_T("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " Data received: " << message.DebugString(true));

                if (message.GetWriteTime() < Self.StartingMessageTimestamp) {
                    SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " Skip data. StartingMessageTimestamp: " << Self.StartingMessageTimestamp << ". Write time: " << message.GetWriteTime());
                    continue;
                }

                auto [item, size] = CreateItem(message);

                auto& curBatch = GetActiveBatch(partitionKey, message.GetWriteTime());
                curBatch.Data.emplace_back(std::move(item));
                curBatch.UsedSpace += size;

                auto& offsets = curBatch.OffsetRanges[message.GetPartitionSession()];
                if (!offsets.empty() && offsets.back().second == message.GetOffset()) {
                    offsets.back().second = message.GetOffset() + 1;
                } else {
                    offsets.emplace_back(message.GetOffset(), message.GetOffset() + 1);
                }
            }
        }

        void operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
            const auto& LogPrefix = Self.LogPrefix;
            TString message = (TStringBuilder() << "Read session to topic \"" << Self.SourceParams.GetTopicPath() << "\" was closed");
            SRC_LOG_E("SessionId: " << Self.GetSessionId() << " " << message << ": " << ev.DebugString());
            TIssue issue(message);
            for (const auto& subIssue : ev.GetIssues()) {
                TIssuePtr newIssue(new TIssue(subIssue));
                issue.AddSubIssue(newIssue);
            }
            Self.Send(Self.ComputeActorId, new TEvAsyncInputError(Self.InputIndex, TIssues({issue}), NYql::NDqProto::StatusIds::BAD_REQUEST));
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) { }

        void operator()(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
            const auto partitionKeyStr = ToString(partitionKey);

            SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " StartPartitionSessionEvent received");

            TMaybe<ui64> readOffset;
            const auto offsetIt = Self.PartitionToOffset.find(partitionKey);
            if (offsetIt != Self.PartitionToOffset.end()) {
                readOffset = offsetIt->second;
            }
            SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " Confirm StartPartitionSession with offset " << readOffset);
            event.Confirm(readOffset);
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
            const auto partitionKeyStr = ToString(partitionKey);
            SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " StopPartitionSessionEvent received");
            event.Confirm();
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
            const auto partitionKeyStr = ToString(partitionKey);
            SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " EndPartitionSessionEvent received");
        }

        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent&) { }

        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
            const auto partitionKeyStr = ToString(partitionKey);
            SRC_LOG_D("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " PartitionSessionClosedEvent received");
        }

        TReadyBatch& GetActiveBatch(const TPartitionKey& partitionKey, TInstant time) {
            if (Y_UNLIKELY(Self.ReadyBuffer.empty() || Self.ReadyBuffer.back().Watermark.Defined())) {
                Self.ReadyBuffer.emplace(Nothing(), BatchCapacity);
            }

            TReadyBatch& activeBatch = Self.ReadyBuffer.back();

            if (!Self.WatermarkTracker) {
                // Watermark tracker disabled => there is no way more than one batch will be used
                return activeBatch;
            }

            const auto maybeNewWatermark = Self.WatermarkTracker->NotifyNewPartitionTime(
                partitionKey,
                time,
                TInstant::Now());
            if (!maybeNewWatermark) {
                // Watermark wasn't moved => use current active batch
                return activeBatch;
            }

            Self.PushWatermarkToReady(*maybeNewWatermark);
            return Self.ReadyBuffer.emplace(Nothing(), BatchCapacity); // And open new batch
        }

        std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            const TString& data = message.GetData();

            i64 usedSpace = 0;
            NUdf::TUnboxedValuePod item;
            if (Self.MetadataFields.empty()) {
                item = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
                usedSpace += data.Size();
            } else {
                NUdf::TUnboxedValue* itemPtr;
                item = Self.HolderFactory.CreateDirectArrayHolder(Self.MetadataFields.size() + 1, itemPtr);
                *(itemPtr++) = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
                usedSpace += data.Size();

                for (const auto& [name, extractor] : Self.MetadataFields) {
                    auto [ub, size] = extractor(message);
                    *(itemPtr++) = std::move(ub);
                    usedSpace += size;
                }
            }

            return std::make_pair(item, usedSpace);
        }

        TDqPqReadActor& Self;
        ui32 BatchCapacity;
        const TString& LogPrefix;
    };

private:
    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    TMetrics Metrics;
    const i64 BufferSize;
    const THolderFactory& HolderFactory;
    const TString LogPrefix;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NPq::NProto::TDqPqTopicSource SourceParams;
    const NPq::NProto::TDqReadTaskParams ReadParams;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    NThreading::TFuture<void> EventFuture;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    TInstant StartingMessageTimestamp;
    const NActors::TActorId ComputeActorId;
    std::queue<std::pair<ui64, NYdb::NTopic::TDeferredCommit>> DeferredCommits;
    NYdb::NTopic::TDeferredCommit CurrentDeferredCommit;
    bool SubscribedOnEvent = false;
    std::vector<std::tuple<TString, TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    std::queue<TReadyBatch> ReadyBuffer;
    TMaybe<TDqSourceWatermarkTracker<TPartitionKey>> WatermarkTracker;
    TMaybe<TInstant> NextIdlenesCheckAt;
};

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NActors::TActorId& computeActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    i64 bufferSize
    )
{
    auto taskParamsIt = taskParams.find("pq");
    YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");

    NPq::NProto::TDqReadTaskParams readTaskParamsMsg;
    YQL_ENSURE(readTaskParamsMsg.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");

    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqReadActor* actor = new TDqPqReadActor(
        inputIndex,
        statsLevel,
        txId,
        taskId,
        holderFactory,
        std::move(settings),
        std::move(readTaskParamsMsg),
        std::move(driver),
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        computeActorId,
        counters,
        bufferSize
    );

    return {actor, actor};
}

void RegisterDqPqReadActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, const ::NMonitoring::TDynamicCounterPtr& counters) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>("PqSource",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
        return CreateDqPqReadActor(
            std::move(settings),
            args.InputIndex,
            args.StatsLevel,
            args.TxId,
            args.TaskId,
            args.SecureParams,
            args.TaskParams,
            driver,
            credentialsFactory,
            args.ComputeActorId,
            args.HolderFactory,
            counters,
            PQReadDefaultFreeSpace);
    });

}

} // namespace NYql::NDq
