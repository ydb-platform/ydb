#include "dq_pq_write_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h> 
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h> 
#include <ydb/library/yql/dq/common/dq_common.h> 
#include <ydb/library/yql/dq/proto/dq_checkpoint.pb.h> 

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h> 
#include <ydb/library/yql/minikql/mkql_alloc.h> 
#include <ydb/library/yql/minikql/mkql_string_util.h> 
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h> 
#include <ydb/library/yql/utils/yql_panic.h> 

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <algorithm>
#include <queue>
#include <variant>

namespace NKikimrServices {
    // using constant value from ydb/core/protos/services.proto
    // but to avoid peerdir on ydb/core/protos we introduce this constant
    constexpr ui32 KQP_COMPUTE = 535;
};

const TString LogPrefix = "PQ sink. ";

#define SINK_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SINK_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

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

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
};

} // namespace

class TDqPqWriteActor : public NActors::TActor<TDqPqWriteActor>, public IDqSinkActor {
public:
    TDqPqWriteActor(
        ui64 outputIndex,
        const TString& txId,
        NPq::NProto::TDqPqTopicSink&& sinkParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        IDqSinkActor::ICallbacks* callbacks,
        i64 freeSpace)
        : TActor<TDqPqWriteActor>(&TDqPqWriteActor::StateFunc)
        , OutputIndex(outputIndex)
        , TxId(txId)
        , SinkParams(std::move(sinkParams))
        , Driver(std::move(driver))
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Callbacks(callbacks)
        , FreeSpace(freeSpace)
        , PersQueueClient(Driver, GetPersQueueClientSettings())
    { }

    static constexpr char ActorName[] = "DQ_PQ_WRITE_ACTOR";

public:
    void SendData(
        NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
        i64 dataSize,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint,
        bool finished) override
    {
        Y_UNUSED(finished);
        Y_UNUSED(dataSize);

        CreateSessionIfNotExists();

        for (const NUdf::TUnboxedValue& item : batch) {
            if (!item.IsBoxed()) {
                Fail("Struct with single field was expected");
                return;
            }

            const NUdf::TUnboxedValue dataCol = item.GetElement(0);

            if (!dataCol.IsString() && !dataCol.IsEmbedded()) {
                Fail(TStringBuilder() << "Non string value could not be written to YDS stream");
                return;
            }

            TString data(dataCol.AsStringRef());

            LWPROBE(PqWriteDataToSend, TxId, SinkParams.GetTopicPath(), data);
            SINK_LOG_T("Received data for sending: " << data);

            const auto messageSize = GetItemSize(data);
            if (messageSize > MaxMessageSize) {
                Fail(TStringBuilder() << "Max message size for YDS is " << MaxMessageSize
                    << " bytes but received message with size of " << messageSize << " bytes");
                return;
            }

            FreeSpace -= messageSize;
            Buffer.push(std::move(data));
        }

        if (checkpoint) {
            if (Buffer.empty()) {
                Callbacks->OnSinkStateSaved(BuildState(), OutputIndex, *checkpoint);
            } else {
                DeferredCheckpoints.emplace(NextSeqNo + Buffer.size() - 1, *checkpoint);
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

    void LoadState(const NDqProto::TSinkState& state) override {
        Y_VERIFY(NextSeqNo == 1);
        const auto& data = state.GetData().GetStateData();
        if (data.GetVersion() == StateVersion) { // Current version
            NPq::NProto::TDqPqTopicSinkState stateProto;
            YQL_ENSURE(stateProto.ParseFromString(data.GetBlob()), "Serialized state is corrupted");
            SourceId = stateProto.GetSourceId();
            ConfirmedSeqNo = stateProto.GetConfirmedSeqNo();
            NextSeqNo = ConfirmedSeqNo + 1;
            return;
        }
        ythrow yexception() << "Invalid state version " << data.GetVersion();
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        Y_UNUSED(checkpoint);
    }

    i64 GetFreeSpace() const override {
        return FreeSpace;
    };

    ui64 GetOutputIndex() const override {
        return OutputIndex;
    };

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvPqEventsReady, Handle);
    )

    void Handle(TEvPrivate::TEvPqEventsReady::TPtr&) {
        while (HandleNewPQEvents()) { }
        SubscribeOnNextEvent();
    }

    // IActor & IDqSinkActor
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

    NYdb::NPersQueue::TWriteSessionSettings GetWriteSessionSettings() {
        return NYdb::NPersQueue::TWriteSessionSettings(SinkParams.GetTopicPath(), GetSourceId())
            .MaxMemoryUsage(FreeSpace)
            .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Auto)
            .Codec(SinkParams.GetClusterType() == NPq::NProto::DataStreams
                ? NYdb::NPersQueue::ECodec::RAW
                : NYdb::NPersQueue::ECodec::GZIP);
    }

    NYdb::NPersQueue::TPersQueueClientSettings GetPersQueueClientSettings() {
        return NYdb::NPersQueue::TPersQueueClientSettings()
            .Database(SinkParams.GetDatabase())
            .DiscoveryEndpoint(SinkParams.GetEndpoint())
            .EnableSsl(SinkParams.GetUseSsl())
            .CredentialsProviderFactory(CredentialsProviderFactory);
    }

    static i64 GetItemSize(const TString& item) {
        return std::max(static_cast<i64>(item.size()), static_cast<i64>(1));
    }

    void CreateSessionIfNotExists() {
        if (!WriteSession) {
            WriteSession = PersQueueClient.CreateWriteSession(GetWriteSessionSettings());
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
            auto issues = std::visit(TPQEventProcessor{*this}, event);
            if (issues) {
                WriteSession->Close(TDuration::Zero());
                WriteSession.reset();
                Callbacks->OnSinkError(OutputIndex, *issues, true);
                break;
            }

            if (ShouldNotifyNewFreeSpace && FreeSpace > 0) {
                Callbacks->ResumeExecution();
                ShouldNotifyNewFreeSpace = false;
            }
        }
        return !events.empty();
    }

    NDqProto::TSinkState BuildState() {
        NPq::NProto::TDqPqTopicSinkState stateProto;
        stateProto.SetSourceId(GetSourceId());
        stateProto.SetConfirmedSeqNo(ConfirmedSeqNo);
        TString serializedState;
        YQL_ENSURE(stateProto.SerializeToString(&serializedState));

        NDqProto::TSinkState sinkState;
        auto* data = sinkState.MutableData()->MutableStateData();
        data->SetVersion(StateVersion);
        data->SetBlob(serializedState);
        return sinkState;
    }

    void WriteNextMessage(NYdb::NPersQueue::TContinuationToken&& token) {
        WriteSession->Write(std::move(token), Buffer.front(), NextSeqNo++);
        WaitingAcks.push(GetItemSize(Buffer.front()));
        Buffer.pop();
    }

    void Fail(TString message) {
        TIssues issues;
        issues.AddIssue(message);
        Callbacks->OnSinkError(OutputIndex, issues, true);
        return;
    }

    struct TPQEventProcessor {
        std::optional<TIssues> operator()(NYdb::NPersQueue::TSessionClosedEvent& ev) {
            TIssues issues;
            issues.AddIssue(TStringBuilder() << "Write session to topic \"" << Self.SinkParams.GetTopicPath() << "\" was closed: " << ev.DebugString());
            return issues;
        }

        std::optional<TIssues> operator()(NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent& ev) {
            if (ev.Acks.empty()) {
                return std::nullopt;
            }

            //Y_VERIFY(Self.ConfirmedSeqNo == 0 || ev.Acks.front().SeqNo == Self.ConfirmedSeqNo + 1);

            for (auto it = ev.Acks.begin(); it != ev.Acks.end(); ++it) {
                //Y_VERIFY(it == ev.Acks.begin() || it->SeqNo == std::prev(it)->SeqNo + 1);

                if (it->State == NYdb::NPersQueue::TWriteSessionEvent::TWriteAck::EEventState::EES_DISCARDED) {
                    TIssues issues;
                    issues.AddIssue(TStringBuilder() << "Message with seqNo " << it->SeqNo << " was discarded");
                    return issues;
                }

                Self.FreeSpace += Self.WaitingAcks.front();
                Self.WaitingAcks.pop();

                if (!Self.DeferredCheckpoints.empty() && std::get<0>(Self.DeferredCheckpoints.front()) == it->SeqNo) {
                    Self.ConfirmedSeqNo = it->SeqNo;
                    Self.Callbacks->OnSinkStateSaved(Self.BuildState(), Self.OutputIndex, std::get<1>(Self.DeferredCheckpoints.front()));
                    Self.DeferredCheckpoints.pop();
                }
            }
            Self.ConfirmedSeqNo = ev.Acks.back().SeqNo;

            return std::nullopt;
        }

        std::optional<TIssues> operator()(NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            //Y_VERIFY(!Self.ContinuationToken);

            if (!Self.Buffer.empty()) {
                Self.WriteNextMessage(std::move(ev.ContinuationToken));
                return std::nullopt;
            }

            Self.ContinuationToken = std::move(ev.ContinuationToken);
            return std::nullopt;
        }

        TDqPqWriteActor& Self;
    };

private:
    const ui64 OutputIndex;
    const TString TxId;
    const NPq::NProto::TDqPqTopicSink SinkParams;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    IDqSinkActor::ICallbacks* const Callbacks;
    i64 FreeSpace = 0;

    NYdb::NPersQueue::TPersQueueClient PersQueueClient;
    std::shared_ptr<NYdb::NPersQueue::IWriteSession> WriteSession;
    TString SourceId;
    ui64 NextSeqNo = 1;
    ui64 ConfirmedSeqNo = 0;
    std::optional<NYdb::NPersQueue::TContinuationToken> ContinuationToken;
    NThreading::TFuture<void> EventFuture;
    std::queue<i64> InflightMessageSizes;
    bool ShouldNotifyNewFreeSpace = false;
    std::queue<TString> Buffer;
    std::queue<i64> WaitingAcks; // Size of items which are waiting for acks (used to update free space)
    std::queue<std::tuple<ui64, NDqProto::TCheckpoint>> DeferredCheckpoints;
};

std::pair<IDqSinkActor*, NActors::IActor*> CreateDqPqWriteActor(
    NPq::NProto::TDqPqTopicSink&& settings,
    ui64 outputIndex,
    TTxId txId,
    const THashMap<TString, TString>& secureParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqSinkActor::ICallbacks* callbacks,
    i64 freeSpace)
{
    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqWriteActor* actor = new TDqPqWriteActor(
        outputIndex,
        std::holds_alternative<ui64>(txId) ? ToString(txId) : std::get<TString>(txId),
        std::move(settings),
        std::move(driver),
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        callbacks,
        freeSpace);
    return {actor, actor};
}

void RegisterDqPqWriteActorFactory(TDqSinkFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.Register<NPq::NProto::TDqPqTopicSink>("PqSink",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory)](
            NPq::NProto::TDqPqTopicSink&& settings,
            IDqSinkActorFactory::TArguments&& args)
        {
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
            return CreateDqPqWriteActor(
                std::move(settings),
                args.OutputIndex,
                args.TxId,
                args.SecureParams,
                driver,
                credentialsFactory,
                args.Callback
            );
        });
}

} // namespace NYql::NDq
