#include "dq_pq_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
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
#include <util/generic/utility.h>

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

class TDqPqReadActor : public NActors::TActor<TDqPqReadActor>, public IDqSourceActor {
public:
    using TPartitionKey = std::pair<TString, ui64>; // Cluster, partition id.

    TDqPqReadActor(
        ui64 inputIndex,
        const TString& txId,
        const THolderFactory& holderFactory,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        ICallbacks* callbacks,
        i64 bufferSize)
        : TActor<TDqPqReadActor>(&TDqPqReadActor::StateFunc)
        , InputIndex(inputIndex)
        , TxId(txId)
        , BufferSize(bufferSize)
        , HolderFactory(holderFactory)
        , Driver(std::move(driver))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , SourceParams(std::move(sourceParams))
        , ReadParams(std::move(readParams))
        , StartingMessageTimestamp(TInstant::Now())
        , Callbacks(callbacks)
    {
        Y_UNUSED(HolderFactory);
    }

    NYdb::NPersQueue::TPersQueueClientSettings GetPersQueueClientSettings() const {
        NYdb::NPersQueue::TPersQueueClientSettings opts;
        opts.Database(SourceParams.GetDatabase())
            .DiscoveryEndpoint(SourceParams.GetEndpoint())
            .EnableSsl(SourceParams.GetUseSsl())
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
            } else {
                ythrow yexception() << "Invalid state version " << data.GetVersion();
            }
        }
        StartingMessageTimestamp = minStartingMessageTs;
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
    };

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
        SubscribedOnEvent = false;
        Y_UNUSED(ev);
        Callbacks->OnNewSourceDataArrived(InputIndex);
    }

    // IActor & IDqSourceActor
    void PassAway() override { // Is called from Compute Actor
        if (ReadSession) {
            ReadSession->Close(TDuration::Zero());
            ReadSession.reset();
        }
        PersQueueClient.reset();
        TActor<TDqPqReadActor>::PassAway();
    }

    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool&, i64 freeSpace) override { 
        auto events = GetReadSession().GetEvents(false, TMaybe<size_t>(), static_cast<size_t>(Max<i64>(freeSpace, 0)));

        ui32 batchSize = 0;
        for (auto& event : events) {
            if (const auto* val = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                batchSize += val->GetMessages().size();
            }
        }
        buffer.clear();
        buffer.reserve(batchSize);

        i64 usedSpace = 0;
        for (auto& event : events) {
            std::visit(TPQEventProcessor{*this, buffer, usedSpace}, event);
        }

        SubscribeOnNextEvent();

        return usedSpace;
    }

private:
    NYdb::NPersQueue::TReadSessionSettings GetReadSessionSettings() const {
        NYdb::NPersQueue::TTopicReadSettings topicReadSettings;
        topicReadSettings.Path(SourceParams.GetTopicPath());
        ui64 currentPartition = ReadParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
        do {
            topicReadSettings.AppendPartitionGroupIds(currentPartition + 1); // 1-based.
            currentPartition += ReadParams.GetPartitioningParams().GetDqPartitionsCount();
        } while (currentPartition < ReadParams.GetPartitioningParams().GetTopicPartitionsCount());

        return NYdb::NPersQueue::TReadSessionSettings()
            .DisableClusterDiscovery(SourceParams.GetClusterType() == NPq::NProto::DataStreams)
            .AppendTopics(topicReadSettings)
            .ConsumerName(SourceParams.GetConsumerName())
            .MaxMemoryUsageBytes(BufferSize)
            .StartingMessageTimestamp(StartingMessageTimestamp);
    }

    void UpdateStateWithNewReadData(const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) {
        if (event.GetMessages().empty()) {
            return;
        }

        assert(MaxElementBy(event.GetMessages(), [](const auto& message){ return message.GetOffset(); })
                ->GetOffset() == event.GetMessages().back().GetOffset());

        const auto maxOffset = event.GetMessages().back().GetOffset();
        PartitionToOffset[MakePartitionKey(event.GetPartitionStream())] = maxOffset + 1; // Next offset to read from.
    }

    static TPartitionKey MakePartitionKey(const NYdb::NPersQueue::TPartitionStream::TPtr& partitionStreamPtr) {
        return std::make_pair(partitionStreamPtr->GetCluster(), partitionStreamPtr->GetPartitionId());
    }

    void SubscribeOnNextEvent() {
        if (!SubscribedOnEvent) {
            SubscribedOnEvent = true;
            NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
            EventFuture = GetReadSession().WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
                actorSystem->Send(selfId, new TEvPrivate::TEvSourceDataReady());
            });
        }
    }

    struct TPQEventProcessor {
        void operator()(NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) {
            for (const auto& message : event.GetMessages()) {
                const TString& data = message.GetData();

                LWPROBE(PqReadDataReceived, Self.TxId, Self.SourceParams.GetTopicPath(), data);
                SINK_LOG_T("Data received: " << data);

                Batch.emplace_back(NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size())));
                UsedSpace += data.Size();
            }
            Self.UpdateStateWithNewReadData(event);
            Self.CurrentDeferredCommit.Add(event);
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

        TDqPqReadActor& Self;
        TUnboxedValueVector& Batch;
        i64& UsedSpace;
    };

private:
    const ui64 InputIndex;
    const TString TxId;
    const i64 BufferSize;
    const THolderFactory& HolderFactory;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NPq::NProto::TDqPqTopicSource SourceParams;
    const NPq::NProto::TDqReadTaskParams ReadParams;
    std::unique_ptr<NYdb::NPersQueue::TPersQueueClient> PersQueueClient;
    std::shared_ptr<NYdb::NPersQueue::IReadSession> ReadSession;
    NThreading::TFuture<void> EventFuture;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    TInstant StartingMessageTimestamp;
    ICallbacks* const Callbacks;
    std::queue<std::pair<ui64, NYdb::NPersQueue::TDeferredCommit>> DeferredCommits;
    NYdb::NPersQueue::TDeferredCommit CurrentDeferredCommit;
    bool SubscribedOnEvent = false;
};

std::pair<IDqSourceActor*, NActors::IActor*> CreateDqPqReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TTxId txId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqSourceActor::ICallbacks* callbacks,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
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
        std::holds_alternative<ui64>(txId) ? ToString(txId) : std::get<TString>(txId),
        holderFactory,
        std::move(settings),
        std::move(readTaskParamsMsg),
        std::move(driver),
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        callbacks,
        bufferSize
    );

    return {actor, actor};
}

void RegisterDqPqReadActorFactory(TDqSourceFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.Register<NPq::NProto::TDqPqTopicSource>("PqSource",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory)](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqSourceActorFactory::TArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
        return CreateDqPqReadActor(
            std::move(settings),
            args.InputIndex,
            args.TxId,
            args.SecureParams,
            args.TaskParams,
            driver,
            credentialsFactory,
            args.Callback,
            args.HolderFactory);
    });

}

} // namespace NYql::NDq
