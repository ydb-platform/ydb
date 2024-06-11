#include "dq_pq_rd_read_actor.h"
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
//#include <ydb/core/fq/libs/row_dispatcher/leader_detector.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>


#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
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

class TDqPqRdReadActor : public NActors::TActorBootstrapped<TDqPqRdReadActor>, public IDqComputeActorAsyncInput {
public:
    using TPartitionKey = std::pair<TString, ui64>; // Cluster, partition id.
    using TDebugOffsets = TMaybe<std::pair<ui64, ui64>>;

private:
    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    //const THolderFactory& HolderFactory;
    const TString LogPrefix;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NPq::NProto::TDqPqTopicSource SourceParams;
    const NPq::NProto::TDqReadTaskParams ReadParams;
    NThreading::TFuture<void> EventFuture;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    TInstant StartingMessageTimestamp;
    const NActors::TActorId ComputeActorId;
    std::queue<std::pair<ui64, NYdb::NTopic::TDeferredCommit>> DeferredCommits;
    NYdb::NTopic::TDeferredCommit CurrentDeferredCommit;
    std::vector<std::tuple<TString, TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    TMaybe<TDqSourceWatermarkTracker<TPartitionKey>> WatermarkTracker;
    TMaybe<TInstant> NextIdlenesCheckAt;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory2;

public:
    TDqPqRdReadActor(
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
        NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory2);

    void Handle(NFq::TEvents::TEvRowDispatcherResult::TPtr &ev);


    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvents::TEvRowDispatcherResult, Handle);
        // hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        // hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        // hFunc(TEvents::TEvUndelivered, Handle);
        // hFunc(NActors::TEvents::TEvWakeup, Handle)
        // hFunc(NFq::TEvRowDispatcher::TEvCoordinatorInfo, Handle);
    })
    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void Bootstrap();
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override;
    void LoadState(const TSourceState& state) override;
    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    ui64 GetInputIndex() const override;
    const TDqAsyncStats& GetIngressStats() const override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    std::vector<ui64> GetPartitionsToRead() const;
};

TDqPqRdReadActor::TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& /*holderFactory*/,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory2)
        : InputIndex(inputIndex)
        , TxId(txId)
        //, HolderFactory(holderFactory)
        , LogPrefix(TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << taskId << ". PQ source. ")
        , Driver(std::move(driver))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , SourceParams(std::move(sourceParams))
        , ReadParams(std::move(readParams))
        , StartingMessageTimestamp(TInstant::MilliSeconds(TInstant::Now().MilliSeconds())) // this field is serialized as milliseconds, so drop microseconds part to be consistent with storage
        , ComputeActorId(computeActorId)
        , CredentialsProviderFactory2(credentialsProviderFactory2)
{
    MetadataFields.reserve(SourceParams.MetadataFieldsSize());
    TPqMetaExtractor fieldsExtractor;
    for (const auto& fieldName : SourceParams.GetMetadataFields()) {
        MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
    }

    IngressStats.Level = statsLevel;
    SRC_LOG_D("TDqPqRdReadActor");
}

void TDqPqRdReadActor::Bootstrap() {
    Become(&TDqPqRdReadActor::StateFunc);
    SRC_LOG_D("TDqPqRdReadActor::Bootstrap");

    //NFq::NConfig::TRowDispatcherCoordinatorConfig config;
    //config.Set

    Send(NFq::RowDispatcherServiceActorId(), new NFq::TEvents::TEvRowDispatcherRequest());
  //  Register(NFq::NewLeaderDetector(SelfId(), config, CredentialsProviderFactory2, Driver).release());
}

void TDqPqRdReadActor::SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) {
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

void TDqPqRdReadActor::LoadState(const TSourceState& state) {
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
        SRC_LOG_D("SessionId: " << " Restoring offset: cluster " << key.first << ", partition id " << key.second << ", offset: " << value);
    }
    StartingMessageTimestamp = minStartingMessageTs;
    IngressStats.Bytes += ingressBytes;
    IngressStats.Chunks++;
}

void TDqPqRdReadActor::CommitState(const NDqProto::TCheckpoint& checkpoint) {
    const auto checkpointId = checkpoint.GetId();
    while (!DeferredCommits.empty() && DeferredCommits.front().first <= checkpointId) {
        auto& deferredCommit = DeferredCommits.front().second;
        deferredCommit.Commit();
        DeferredCommits.pop();
    }
}

ui64 TDqPqRdReadActor::GetInputIndex() const {
    return InputIndex;
}

const TDqAsyncStats& TDqPqRdReadActor::GetIngressStats() const {
    return IngressStats;
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor

    TActorBootstrapped<TDqPqRdReadActor>::PassAway();
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& /*buffer*/, TMaybe<TInstant>& /*watermark*/, bool&, i64 freeSpace) {
    SRC_LOG_D("SessionId: " << " GetAsyncInputData freeSpace = " << freeSpace);


    return 0;
}

std::vector<ui64> TDqPqRdReadActor::GetPartitionsToRead() const {
    std::vector<ui64> res;

    ui64 currentPartition = ReadParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
    do {
        res.emplace_back(currentPartition); // 0-based in topic API
        currentPartition += ReadParams.GetPartitioningParams().GetDqPartitionsCount();
    } while (currentPartition < ReadParams.GetPartitioningParams().GetTopicPartitionsCount());

    return res;
}

void TDqPqRdReadActor::Handle(NFq::TEvents::TEvRowDispatcherResult::TPtr &ev) {
        SRC_LOG_D("TEvRowDispatcherResult = " << ev->Get()->CoordinatorActorId);

}


// void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
//     SRC_LOG_D("TDqPqRdReadActor :: TEvCoordinatorChanged new leader " << ev->Get()->LeaderActorId);
// }

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
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
    NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory)
{
    auto taskParamsIt = taskParams.find("pq");
    YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");

    NPq::NProto::TDqReadTaskParams readTaskParamsMsg;
    YQL_ENSURE(readTaskParamsMsg.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");

    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqRdReadActor* actor = new TDqPqRdReadActor(
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
        credentialsProviderFactory
    );

    return {actor, actor};
}

void RegisterDqPqRdReadActorFactory(
    TDqAsyncIoFactory& factory,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>("PqRdSource",
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), credentialsProviderFactory = std::move(credentialsProviderFactory)](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
        return CreateDqPqRdReadActor(
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
            credentialsProviderFactory);
    });

}

} // namespace NYql::NDq
