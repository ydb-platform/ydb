#include "dq_pq_write_actor.h"
#include "probes.h"

#include <ydb/core/base/appdata_fwd.h>

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

#define LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, s)
#define LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, s)
#define LOG_PRIO(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, s)


#define SINK_LOG_T(s) LOG_T(LogPrefix << s)
#define SINK_LOG_D(s) LOG_D(LogPrefix << s)
#define SINK_LOG_I(s) LOG_I(LogPrefix << s)
#define SINK_LOG_W(s) LOG_W(LogPrefix << s)
#define SINK_LOG_N(s) LOG_N(LogPrefix << s)
#define SINK_LOG_E(s) LOG_E(LogPrefix << s)
#define SINK_LOG_C(s) LOG_C(LogPrefix << s)
#define SINK_LOG_PRIO(prio, s) LOG_PRIO(prio, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

constexpr ui32 StateVersion = 1;
constexpr ui32 MaxMessageSize = 1_MB;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvPqEventsReady = EvBegin,
        EvExecuteTopicEvent,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};

    struct TEvExecuteTopicEvent : public TTopicEventBase<TEvExecuteTopicEvent, EvExecuteTopicEvent> {
        using TTopicEventBase::TTopicEventBase;
    };
};

TString MakeStringForLog(const NDqProto::TCheckpoint& checkpoint) {
    return TStringBuilder() << "[Checkpoint " << checkpoint.GetGeneration() << "." << checkpoint.GetId() << "] ";
}

} // anonymous namespace

class TDqPqWriteActor : public NActors::TActor<TDqPqWriteActor>, public IDqComputeActorAsyncOutput, TTopicEventProcessor<TEvPrivate::TEvExecuteTopicEvent> {
    struct TMetrics {
        TMetrics(const TTxId& txId, ui64 taskId, const ::NMonitoring::TDynamicCounterPtr& counters, bool enableStreamingQueriesCounters)
            : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
            , Counters(counters) {
            if (Counters) {
                SubGroup = Counters->GetSubgroup("sink", "PqSink");
            } else {
                SubGroup = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            }
            auto task = SubGroup;
            if (enableStreamingQueriesCounters) {
                auto sink = SubGroup->GetSubgroup("tx_id", TxId);
                task = sink->GetSubgroup("task_id", ToString(taskId));
            }
            LastAckLatency = task->GetCounter("LastAckLatencyMs");
            InFlyCheckpoints = task->GetCounter("InFlyCheckpoints");
            InFlyData = task->GetCounter("InFlyData");
            AlreadyWritten = task->GetCounter("AlreadyWritten");
            FirstContinuationTokenMs = task->GetCounter("FirstContinuationTokenMs");
            EgressDataRate = task->GetCounter("EgressDataRate", true);
        }

        ~TMetrics() {
            SubGroup->RemoveSubgroup("tx_id", TxId);
        }

        TString TxId;
        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounterPtr SubGroup;
        ::NMonitoring::TDynamicCounters::TCounterPtr LastAckLatency;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlyCheckpoints;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlyData;
        ::NMonitoring::TDynamicCounters::TCounterPtr AlreadyWritten;
        ::NMonitoring::TDynamicCounters::TCounterPtr FirstContinuationTokenMs;
        ::NMonitoring::TDynamicCounters::TCounterPtr EgressDataRate;
    };

    struct TAckInfo {
        TAckInfo(i64 messageSize, const TInstant& startTime, ui64 seqNo)
            : MessageSize(messageSize)
            , StartTime(startTime)
            , SeqNo(seqNo)
        {}

        i64 MessageSize = 0;
        TInstant StartTime;
        ui64 SeqNo = 0;
    };

public:
    TDqPqWriteActor(
        ui64 outputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        NPq::NProto::TDqPqTopicSink&& sinkParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 freeSpace,
        const IPqGateway::TPtr& pqGateway,
        bool enableStreamingQueriesCounters,
        bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag)
        : TActor<TDqPqWriteActor>(&TDqPqWriteActor::StateFunc)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , Metrics(txId, taskId, counters, enableStreamingQueriesCounters)
        , SinkParams(std::move(sinkParams))
        , Driver(std::move(driver))
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Callbacks(callbacks)
        , LogPrefix(TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", TaskId: " << taskId << ", PQ sink. ")
        , FreeSpace(freeSpace)
        , PqGateway(pqGateway)
        , TaskId(taskId)
        , EnableDeduplication(enableStreamingQueriesPqSinkDeduplicationFeatureFlag && SinkParams.GetEnableDeduplication())
    { 
        EgressStats.Level = statsLevel;
    }

    static constexpr char ActorName[] = "DQ_PQ_WRITE_ACTOR";

public:
    void SendData(
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
        i64 dataSize,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint,
        bool finished) override
    {
        SINK_LOG_T("SendData. Batch: " << batch.RowCount()
            << ". Checkpoint: " << checkpoint.Defined()
            << ". Finished: " << finished);
        Y_UNUSED(dataSize);

        if (finished) {
            Finished = true;
        }

        CreateSessionIfNotExists();

        Y_ABORT_UNLESS(!batch.IsWide(), "Wide batch is not supported");
        if (!batch.ForEachRow([&](const auto& value) {
            if (!value.IsBoxed()) {
                Fail("Struct with single field was expected");
                return false;
            }

            const NUdf::TUnboxedValue dataCol = value.GetElement(0);

            if (!dataCol.IsString() && !dataCol.IsEmbedded()) {
                Fail(TStringBuilder() << "Non string value could not be written to YDS stream");
                return false;
            }

            TString data(dataCol.AsStringRef());

            LWPROBE(PqWriteDataToSend, TString(TStringBuilder() << TxId), SinkParams.GetTopicPath(), data);
            SINK_LOG_T("Received data for sending: " << data);

            const auto messageSize = GetItemSize(data);
            if (messageSize > MaxMessageSize) {
                Fail(TStringBuilder() << "Max message size for YDS is " << MaxMessageSize
                    << " bytes but received message with size of " << messageSize << " bytes");
                return false;
            }

            FreeSpace -= messageSize;
            Metrics.InFlyData->Inc();
            Buffer.push(std::move(data));
            return true;
        })) {
            return;
        }

        if (checkpoint) {
            if (Buffer.empty() && WaitingAcks.empty()) {
                SINK_LOG_D(MakeStringForLog(*checkpoint) << "Send checkpoint state immediately");
                Callbacks->OnAsyncOutputStateSaved(BuildState(*checkpoint), OutputIndex, *checkpoint);
            } else {
                ui64 seqNo = NextSeqNo + Buffer.size() - 1;
                SINK_LOG_D(MakeStringForLog(*checkpoint) << "Defer sending the checkpoint, seqNo: " << seqNo);
                Metrics.InFlyCheckpoints->Inc();
                DeferredCheckpoints.emplace(seqNo, *checkpoint);
            }
        }

        if (!Buffer.empty() && ContinuationToken) {
            WriteNextMessage(std::move(*ContinuationToken));
            ContinuationToken = std::nullopt;
        }

        while (HandleNewPQEvents()) { } // Write messages while new continuationTokens are arriving

        if (FreeSpace <= 0) {
            ShouldNotifyNewFreeSpace = true;
        }
    };

    void LoadState(const TSinkState& state) override {
        Y_ABORT_UNLESS(NextSeqNo == 1);
        const auto& data = state.Data;
        if (data.Version == StateVersion) { // Current version
            NPq::NProto::TDqPqTopicSinkState stateProto;
            YQL_ENSURE(stateProto.ParseFromString(data.Blob), "Serialized state is corrupted");
            SINK_LOG_D("Load state: " << stateProto);
            SourceId = stateProto.GetSourceId();
            ConfirmedSeqNo = stateProto.GetConfirmedSeqNo();
            NextSeqNo = ConfirmedSeqNo + 1;
            EgressStats.Bytes = stateProto.GetEgressBytes();
            return;
        }
        ythrow yexception() << "Invalid state version " << data.Version;
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        Y_UNUSED(checkpoint);
    }

    i64 GetFreeSpace() const override {
        return FreeSpace;
    }

    ui64 GetOutputIndex() const override {
        return OutputIndex;
    }

    const TDqAsyncStats& GetEgressStats() const override {
        return EgressStats;
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(TEvPrivate::TEvExecuteTopicEvent, HandleTopicEvent);
    )

    void Handle(TEvPrivate::TEvPqEventsReady::TPtr&) {
        if (!Inited) {
            Init();
            Inited = true;
        }
        while (HandleNewPQEvents()) { }
        SubscribeOnNextEvent();
    }

    void Init() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", TaskId: " << TaskId << ", PQ sink. ";
    }

    // IActor & IDqComputeActorAsyncOutput
    void PassAway() override { // Is called from Compute Actor
        if (WriteSession) {
            WriteSession->Close(TDuration::Zero());
        }
        TActor<TDqPqWriteActor>::PassAway();
    }

private:
    const TString& GetSourceId() {
        if (!SourceId) {
            SourceId = CreateGuidAsString(); // Not loaded from state, so this is the first run.
        }
        return SourceId;
    }

    NYdb::NTopic::TWriteSessionSettings GetWriteSessionSettings() {
        auto settings = NYdb::NTopic::TWriteSessionSettings()
            .Path(SinkParams.GetTopicPath())
            .TraceId(LogPrefix)
            .MaxMemoryUsage(FreeSpace)
            .Codec(SinkParams.GetClusterType() == NPq::NProto::DataStreams
                ? NYdb::NTopic::ECodec::RAW
                : NYdb::NTopic::ECodec::GZIP);

        settings.DeduplicationEnabled(EnableDeduplication);
        if (EnableDeduplication) {
            settings.ProducerId(GetSourceId());
            settings.MessageGroupId(GetSourceId());
        }
        return settings;
    }

    IFederatedTopicClient& GetFederatedTopicClient() {
        if (!FederatedTopicClient) {
            FederatedTopicClient = PqGateway->GetFederatedTopicClient(Driver, GetFederatedTopicClientSettings());
        }
        return *FederatedTopicClient;
    }

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() {
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

    static i64 GetItemSize(const TString& item) {
        return std::max(static_cast<i64>(item.size()), static_cast<i64>(1));
    }

    void CreateSessionIfNotExists() {
        if (!WriteSession) {
            WriteSession = GetFederatedTopicClient().CreateWriteSession(GetWriteSessionSettings());
            SubscribeOnNextEvent();
        }
    }

    void SubscribeOnNextEvent() {
        if (!WriteSession) {
            return;
        }

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        EventFuture = WriteSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
            actorSystem->Send(selfId, new TEvPrivate::TEvPqEventsReady());
        });
    }

    bool HandleNewPQEvents() {
        if (!WriteSession) {
            return false;
        }

        auto events = WriteSession->GetEvents();
        for (auto& event : events) {
            auto issues = std::visit(TTopicEventProcessor{*this}, event);
            if (issues) {
                WriteSession->Close(TDuration::Zero());
                WriteSession.reset();
                Callbacks->OnAsyncOutputError(OutputIndex, *issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
                break;
            }

            if (ShouldNotifyNewFreeSpace && FreeSpace > 0) {
                Callbacks->ResumeExecution();
                ShouldNotifyNewFreeSpace = false;
            }
        }
        CheckFinished();
        return !events.empty();
    }

    TSinkState BuildState(const NDqProto::TCheckpoint& checkpoint) {
        NPq::NProto::TDqPqTopicSinkState stateProto;
        stateProto.SetSourceId(GetSourceId());
        stateProto.SetConfirmedSeqNo(ConfirmedSeqNo);
        stateProto.SetEgressBytes(EgressStats.Bytes);
        TString serializedState;
        YQL_ENSURE(stateProto.SerializeToString(&serializedState));

        TSinkState sinkState;
        auto& data = sinkState.Data;
        data.Version = StateVersion;
        data.Blob = serializedState;
        SINK_LOG_T("Save checkpoint " << checkpoint << " state: " << stateProto);
        return sinkState;
    }

    void WriteNextMessage(NYdb::NTopic::TContinuationToken&& token) {
        std::optional<uint64_t> seqNo;
        if (EnableDeduplication) {
            seqNo = NextSeqNo;
        }
        WriteSession->Write(std::move(token), Buffer.front(), seqNo);
        auto itemSize = GetItemSize(Buffer.front());
        WaitingAcks.emplace(itemSize, TInstant::Now(), NextSeqNo);
        NextSeqNo++;
        EgressStats.Bytes += itemSize;
        Metrics.EgressDataRate->Add(itemSize);
        Buffer.pop();
    }

    void Fail(TString message) {
        TIssues issues;
        issues.AddIssue(message);
        Callbacks->OnAsyncOutputError(OutputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
    }

    struct TTopicEventProcessor {
        std::optional<TIssues> operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
            TIssues issues;
            issues.AddIssue(TStringBuilder() << "Write session to topic \"" << Self.SinkParams.GetTopicPath() << "\" was closed: " << ev.DebugString());
            return issues;
        }

        std::optional<TIssues> operator()(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            if (ev.Acks.empty()) {
                LOG_D(Self.LogPrefix << "Empty ack");
                return std::nullopt;
            }

            //Y_ABORT_UNLESS(Self.ConfirmedSeqNo == 0 || ev.Acks.front().SeqNo == Self.ConfirmedSeqNo + 1);

            for (auto it = ev.Acks.begin(); it != ev.Acks.end(); ++it) {
                //Y_ABORT_UNLESS(it == ev.Acks.begin() || it->SeqNo == std::prev(it)->SeqNo + 1);
                LOG_T(Self.LogPrefix << "Ack seq no (from TAcksEvent) " << it->SeqNo);
                if (it->State == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_DISCARDED) {
                    TIssues issues;
                    issues.AddIssue(TStringBuilder() << "Message with seqNo " << it->SeqNo << " was discarded");
                    return issues;
                }

                if (it->State == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_ALREADY_WRITTEN) {
                    Self.Metrics.AlreadyWritten->Inc();
                }

                const auto& ackInfo = Self.WaitingAcks.front();
                Self.Metrics.LastAckLatency->Set((TInstant::Now() - ackInfo.StartTime).MilliSeconds());
                Self.Metrics.InFlyData->Dec();
                Self.FreeSpace += ackInfo.MessageSize;
                ui64 seqNo = ackInfo.SeqNo;        // use seqNo stored on our side because without deduplication we do not specify SeqNo on Write().
                LOG_T(Self.LogPrefix << "Ack seq no (from WaitingAcks) " << seqNo);
                Self.WaitingAcks.pop();

                if (!Self.DeferredCheckpoints.empty() && std::get<0>(Self.DeferredCheckpoints.front()) == seqNo) {
                    Self.ConfirmedSeqNo = seqNo;
                    const auto& checkpoint = std::get<1>(Self.DeferredCheckpoints.front());
                    LOG_D(Self.LogPrefix << MakeStringForLog(checkpoint) << "Send a deferred checkpoint, seqNo: " << seqNo);
                    Self.Callbacks->OnAsyncOutputStateSaved(Self.BuildState(checkpoint), Self.OutputIndex, checkpoint);
                    Self.DeferredCheckpoints.pop();
                    Self.Metrics.InFlyCheckpoints->Dec();
                }
            }
            Self.ConfirmedSeqNo = ev.Acks.back().SeqNo;

            return std::nullopt;
        }

        std::optional<TIssues> operator()(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            //Y_ABORT_UNLESS(!Self.ContinuationToken);

            if (*Self.Metrics.FirstContinuationTokenMs == 0) {
                Self.Metrics.FirstContinuationTokenMs->Set((TInstant::Now() - Self.StartTime).MilliSeconds());
            }

            if (!Self.Buffer.empty()) {
                Self.WriteNextMessage(std::move(ev.ContinuationToken));
                return std::nullopt;
            }

            Self.ContinuationToken = std::move(ev.ContinuationToken);
            return std::nullopt;
        }

        TDqPqWriteActor& Self;
    };

    void CheckFinished() {
        if (Finished && Buffer.empty() && WaitingAcks.empty()) {
            Callbacks->OnAsyncOutputFinished(OutputIndex);
        }
    }

private:
    TInstant StartTime = TInstant::Now();
    const ui64 OutputIndex;
    TDqAsyncStats EgressStats;
    const TTxId TxId;
    TMetrics Metrics;
    const NPq::NProto::TDqPqTopicSink SinkParams;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    IDqComputeActorAsyncOutput::ICallbacks* const Callbacks;
    TString LogPrefix;
    i64 FreeSpace = 0;
    bool Finished = false;

    IFederatedTopicClient::TPtr FederatedTopicClient;
    std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;
    TString SourceId;
    ui64 NextSeqNo = 1;
    ui64 ConfirmedSeqNo = 0;
    std::optional<NYdb::NTopic::TContinuationToken> ContinuationToken;
    NThreading::TFuture<void> EventFuture;
    bool ShouldNotifyNewFreeSpace = false;
    std::queue<TString> Buffer;
    std::queue<TAckInfo> WaitingAcks; // Size of items which are waiting for acks (used to update free space)
    std::queue<std::tuple<ui64, NDqProto::TCheckpoint>> DeferredCheckpoints;
    IPqGateway::TPtr PqGateway;
    ui64 TaskId;
    bool Inited = false;
    bool EnableDeduplication = false;
};

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqPqWriteActor(
    NPq::NProto::TDqPqTopicSink&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    IPqGateway::TPtr pqGateway,
    bool enableStreamingQueriesCounters,
    i64 freeSpace,
    bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag)
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
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        callbacks,
        counters,
        freeSpace,
        pqGateway,
        enableStreamingQueriesCounters,
        enableStreamingQueriesPqSinkDeduplicationFeatureFlag);
    return {actor, actor};
}

void RegisterDqPqWriteActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, const IPqGateway::TPtr& pqGateway, const ::NMonitoring::TDynamicCounterPtr& counters, bool enableStreamingQueriesCounters, bool enableStreamingQueriesPqSinkDeduplicationFeatureFlag) {
    factory.RegisterSink<NPq::NProto::TDqPqTopicSink>("PqSink",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), counters, pqGateway, enableStreamingQueriesCounters, enableStreamingQueriesPqSinkDeduplicationFeatureFlag](
            NPq::NProto::TDqPqTopicSink&& settings,
            IDqAsyncIoFactory::TSinkArguments&& args)
        {
            auto txId = args.TxId;
            auto taskParamsIt = args.TaskParams.find("query_path");
            if (taskParamsIt != args.TaskParams.end()) {
                txId = taskParamsIt->second;
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
                enableStreamingQueriesPqSinkDeduplicationFeatureFlag
            );
        });
}

} // namespace NYql::NDq
