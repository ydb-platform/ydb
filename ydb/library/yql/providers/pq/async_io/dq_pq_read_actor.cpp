#include "dq_pq_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_checkpoint.pb.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>

#include <queue>
#include <variant>

namespace NKikimrServices {
    // using constant value from ydb/core/protos/services.proto
    // but to avoid peerdir on ydb/core/protos we introduce this constant
    constexpr ui32 KQP_COMPUTE = 535;
};

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
        i64 bufferSize,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        bool rangesMode)
        : TActor<TDqPqReadActor>(&TDqPqReadActor::StateFunc)
        , InputIndex(inputIndex)
        , TxId(txId)
        , Metrics(txId, taskId, counters)
        , BufferSize(bufferSize)
        , RangesMode(rangesMode)
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

    NYdb::NPersQueue::TPersQueueClientSettings GetPersQueueClientSettings() const {
        NYdb::NPersQueue::TPersQueueClientSettings opts;
        opts.Database(SourceParams.GetDatabase())
            .DiscoveryEndpoint(SourceParams.GetEndpoint())
            .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsProviderFactory);

        return opts;
    }

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

public:
    void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TSourceState& state) override {
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

        auto* data = state.AddData()->MutableStateData();
        data->SetVersion(StateVersion);
        data->SetBlob(stateBlob);

        DeferredCommits.emplace(checkpoint.GetId(), std::move(CurrentDeferredCommit));
        CurrentDeferredCommit = NYdb::NPersQueue::TDeferredCommit();
    }

    void LoadState(const NDqProto::TSourceState& state) override {
        TInstant minStartingMessageTs = state.DataSize() ? TInstant::Max() : StartingMessageTimestamp;
        ui64 ingressBytes = 0;
        for (const auto& stateData : state.GetData()) {
            const auto& data = stateData.GetStateData();
            if (data.GetVersion() == StateVersion) { // Current version
                NPq::NProto::TDqPqTopicSourceState stateProto;
                YQL_ENSURE(stateProto.ParseFromString(data.GetBlob()), "Serialized state is corrupted");
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
                ythrow yexception() << "Invalid state version " << data.GetVersion();
            }
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
            DeferredCommits.front().second.Commit();
            DeferredCommits.pop();
        }
    }

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    NYdb::NPersQueue::TPersQueueClient& GetPersQueueClient() {
        if (!PersQueueClient) {
            PersQueueClient = std::make_unique<NYdb::NPersQueue::TPersQueueClient>(Driver, GetPersQueueClientSettings());
        }
        return *PersQueueClient;
    }

    NYdb::NPersQueue::IReadSession& GetReadSession() {
        if (!ReadSession) {
            ReadSession = GetPersQueueClient().CreateReadSession(GetReadSessionSettings());
        }
        return *ReadSession;
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSourceDataReady, Handle);
    )

    void Handle(TEvPrivate::TEvSourceDataReady::TPtr& ev) {
        SRC_LOG_T("Source data ready");
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
        PersQueueClient.reset();
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
            SRC_LOG_T("Next idleness check scheduled at " << *nextIdleCheckAt);
            Schedule(*nextIdleCheckAt, new TEvPrivate::TEvSourceDataReady());
        }
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override {
        Metrics.InFlyAsyncInputData->Set(0);
        SRC_LOG_T("GetAsyncInputData freeSpace = " << freeSpace);

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
                if (const auto* val = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    batchItemsEstimatedCount += val->GetMessages().size();
                }
            }

            for (auto& event : events) {
                std::visit(TPQEventProcessor{*this, batchItemsEstimatedCount, LogPrefix}, event);
            }
        }

        if (WatermarkTracker) {
            const auto watermark = WatermarkTracker->HandleIdleness(now);

            if (watermark) {
                const auto t = watermark;
                SRC_LOG_T("Fake watermark " << t << " was produced");
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
            res.emplace_back(currentPartition + 1); // 1-based.
            currentPartition += ReadParams.GetPartitioningParams().GetDqPartitionsCount();
        } while (currentPartition < ReadParams.GetPartitioningParams().GetTopicPartitionsCount());

        return res;
    }

    void InitWatermarkTracker() {
        SRC_LOG_D("Watermarks enabled: " << SourceParams.GetWatermarks().GetEnabled() << " granularity: "
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

    NYdb::NPersQueue::TReadSessionSettings GetReadSessionSettings() const {
        NYdb::NPersQueue::TTopicReadSettings topicReadSettings;
        topicReadSettings.Path(SourceParams.GetTopicPath());
        for (const auto partitionId : GetPartitionsToRead()) {
            topicReadSettings.AppendPartitionGroupIds(partitionId);
        }

        return NYdb::NPersQueue::TReadSessionSettings()
            .DisableClusterDiscovery(SourceParams.GetClusterType() == NPq::NProto::DataStreams)
            .AppendTopics(topicReadSettings)
            .ConsumerName(SourceParams.GetConsumerName())
            .MaxMemoryUsageBytes(BufferSize)
            .StartingMessageTimestamp(StartingMessageTimestamp)
            .RangesMode(RangesMode);
    }

    static TPartitionKey MakePartitionKey(const NYdb::NPersQueue::TPartitionStream::TPtr& partitionStreamPtr) {
        return std::make_pair(partitionStreamPtr->GetCluster(), partitionStreamPtr->GetPartitionId());
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
        TReadyBatch(TMaybe<TInstant> watermark, ui32 dataCapacity) : Watermark(watermark){
            Data.reserve(dataCapacity);
        }

    public:
        TMaybe<TInstant> Watermark;
        TUnboxedValueVector Data;
        i64 UsedSpace = 0;
        THashMap<NYdb::NPersQueue::TPartitionStream::TPtr, TList<std::pair<ui64, ui64>>> OffsetRanges; // [start, end)
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

        for (const auto& [partitionStream, ranges] : readyBatch.OffsetRanges) {
            for (const auto& [start, end] : ranges) {
                CurrentDeferredCommit.Add(partitionStream, start, end);
            }
            PartitionToOffset[MakePartitionKey(partitionStream)] = ranges.back().second;
        }

        ReadyBuffer.pop();

        if (ReadyBuffer.empty()) {
            SubscribeOnNextEvent();
        } else {
            Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
        }

        SRC_LOG_T("Return ready batch."
            << " DataCount = " << buffer.RowCount()
            << " Watermark = " << (watermark ? ToString(*watermark) : "none")
            << " Used space = " << usedSpace);
        return true;
    }

    void PushWatermarkToReady(TInstant watermark) {
        SRC_LOG_D("New watermark " << watermark << " was generated");

        if (Y_UNLIKELY(ReadyBuffer.empty() || ReadyBuffer.back().Watermark.Defined())) {
            ReadyBuffer.emplace(watermark, 0);
            return;
        }

        ReadyBuffer.back().Watermark = watermark;
    }

    struct TPQEventProcessor {
        void operator()(NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) {
            const auto partitionKey = MakePartitionKey(event.GetPartitionStream());
            for (const auto& message : event.GetMessages()) {
                const TString& data = message.GetData();
                Self.IngressStats.Bytes += data.size();
                LWPROBE(PqReadDataReceived, TString(TStringBuilder() << Self.TxId), Self.SourceParams.GetTopicPath(), data);
                SRC_LOG_T("Data received: " << message.DebugString(true));

                if (message.GetWriteTime() < Self.StartingMessageTimestamp) {
                    SRC_LOG_D("Skip data. StartingMessageTimestamp: " << Self.StartingMessageTimestamp << ". Write time: " << message.GetWriteTime());
                    continue;
                }

                auto [item, size] = CreateItem(message);

                auto& curBatch = GetActiveBatch(partitionKey, message.GetWriteTime());
                curBatch.Data.emplace_back(std::move(item));
                curBatch.UsedSpace += size;

                auto& offsets = curBatch.OffsetRanges[message.GetPartitionStream()];
                if (!offsets.empty() && offsets.back().second == message.GetOffset()) {
                    offsets.back().second = message.GetOffset() + 1;
                } else {
                    offsets.emplace_back(message.GetOffset(), message.GetOffset() + 1);
                }
            }
        }

        void operator()(NYdb::NPersQueue::TSessionClosedEvent& ev) {
            ythrow yexception() << "Read session to topic \"" << Self.SourceParams.GetTopicPath()
                << "\" was closed: " << ev.DebugString();
        }

        void operator()(NYdb::NPersQueue::TReadSessionEvent::TCommitAcknowledgementEvent&) { }

        void operator()(NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent& event) {
            TMaybe<ui64> readOffset;
            const auto offsetIt = Self.PartitionToOffset.find(MakePartitionKey(event.GetPartitionStream()));
            if (offsetIt != Self.PartitionToOffset.end()) {
                readOffset = offsetIt->second;
            }
            event.Confirm(readOffset);
        }

        void operator()(NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
            event.Confirm();
        }

        void operator()(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamStatusEvent&) { }

        void operator()(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent&) { }

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

        std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
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
    const bool RangesMode;
    const THolderFactory& HolderFactory;
    const TString LogPrefix;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NPq::NProto::TDqPqTopicSource SourceParams;
    const NPq::NProto::TDqReadTaskParams ReadParams;
    std::unique_ptr<NYdb::NPersQueue::TPersQueueClient> PersQueueClient;
    std::shared_ptr<NYdb::NPersQueue::IReadSession> ReadSession;
    NThreading::TFuture<void> EventFuture;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    TInstant StartingMessageTimestamp;
    const NActors::TActorId ComputeActorId;
    std::queue<std::pair<ui64, NYdb::NPersQueue::TDeferredCommit>> DeferredCommits;
    NYdb::NPersQueue::TDeferredCommit CurrentDeferredCommit;
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
    i64 bufferSize,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    bool rangesMode
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
        bufferSize,
        counters,
        rangesMode
    );

    return {actor, actor};
}

void RegisterDqPqReadActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, const ::NMonitoring::TDynamicCounterPtr& counters, bool rangesMode) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>("PqSource",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters, rangesMode](
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
            PQReadDefaultFreeSpace,
            counters,
            rangesMode);
    });

}

} // namespace NYql::NDq
