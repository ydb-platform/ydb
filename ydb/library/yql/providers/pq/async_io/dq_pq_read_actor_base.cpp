#include "dq_pq_read_actor_base.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/interop/cast.h>

#define SRC_LOG_T(s) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq::NInternal {

namespace {

TInstant TrimToMillis(TInstant instant) {
    return TInstant::MilliSeconds(instant.MilliSeconds());
}

// StartingMessageTimestamp is serialized as milliseconds, so drop microseconds part to be consistent with storage
TInstant InitStartingMessageTimestamp(const NPq::NProto::TDqPqTopicSource& source) {
    const auto& disposition = source.GetDisposition();
    return TrimToMillis([&]() -> TInstant {
        switch (disposition.GetDispositionCase()) {
            case NPq::NProto::StreamingDisposition::kOldest:
                return TInstant::Zero();
            case NPq::NProto::StreamingDisposition::kFresh:
                return TInstant::Now();
            case NPq::NProto::StreamingDisposition::kFromTime:
                return NProtoInterop::CastFromProto(disposition.from_time().timestamp());
            case NPq::NProto::StreamingDisposition::kTimeAgo:
                return TInstant::Now() - NProtoInterop::CastFromProto(disposition.time_ago().duration());
            case NPq::NProto::StreamingDisposition::kFromLastCheckpoint:
                [[fallthrough]];
            case NPq::NProto::StreamingDisposition::DISPOSITION_NOT_SET:
                return TInstant::Now();
        }
    }());
}

class TControlPlaneInteractor final : public NActors::TActorBootstrapped<TControlPlaneInteractor>, public NActors::IActorExceptionHandler {
    using TConnection = NPq::NProto::TEvDescribeConsumer::TConnection;
    using TComplete = std::function<void(TIssues)>;

    struct TEvRewindFinished : NActors::TEventLocal<TEvRewindFinished, TPqControlPlaneEvents::EvEnd> {
        TEvRewindFinished(ui64 partitionId, NYdb::TAsyncStatus result)
            : PartitionId(partitionId)
            , Result(std::move(result))
        {}

        const ui64 PartitionId;
        const NYdb::TAsyncStatus Result;
    };

public:
    TControlPlaneInteractor(NActors::TActorId readerId, NActors::TActorId controlPlaneActorId, TConnection connection, ITopicClient::TPtr topicClient,
        THashSet<ui64> partitions, TString logPrefix, TComplete complete)
        : ReaderId(readerId)
        , ControlPlaneActorId(controlPlaneActorId)
        , Connection(std::move(connection))
        , TopicClient(std::move(topicClient))
        , PartitionsToRead(std::move(partitions))
        , LogPrefix(std::move(logPrefix))
        , Complete(std::move(complete))
    {
        Y_VALIDATE(TopicClient, "Missing topic client for consumer rewind");
    }

    static bool NeedsRewind(const NPq::NProto::TDqPqTopicSource& source, const TInstant startingTimestamp) {
        if (!source.GetAllowConsumerRewindForDisposition() || source.GetConsumerName().empty()) {
            return false;
        }

        switch (source.GetDisposition().GetDispositionCase()) {
            case NPq::NProto::StreamingDisposition::DISPOSITION_NOT_SET:
            case NPq::NProto::StreamingDisposition::kFresh:
            case NPq::NProto::StreamingDisposition::kFromLastCheckpoint:
                return false;
            case NPq::NProto::StreamingDisposition::kFromTime:
                return startingTimestamp <= TInstant::Now();
            default:
                return true;
        }
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Y_VALIDATE(ControlPlaneActorId, "Missing PQ control-plane actor");

        auto request = MakeHolder<TPqControlPlaneEvents::TEvDescribeConsumer>();
        *request->Record.MutableConnection() = Connection;
        for (const auto partitionId : PartitionsToRead) {
            request->Record.AddPartitionIds(partitionId);
        }

        TEventFlags flags = NActors::IEventHandle::FlagTrackDelivery;
        if (ControlPlaneActorId.NodeId() != SelfId().NodeId()) {
            ControlPlaneSubscribed = true;
            flags |= NActors::IEventHandle::FlagSubscribeOnSession;
        }

        Send(ControlPlaneActorId, request.Release(), flags);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TPqControlPlaneEvents::TEvDescribeConsumerResult, Handle);
        hFunc(TEvRewindFinished, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        IgnoreFunc(NActors::TEvInterconnect::TEvNodeConnected);
    )

private:
    bool OnUnhandledException(const std::exception& ex) final {
        Fail(ex.what(), NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, {}));
        return true;
    }

    void PassAway() override {
        if (ControlPlaneSubscribed) {
            Send(NActors::TActivationContext::InterconnectProxy(ControlPlaneActorId.NodeId()), new NActors::TEvents::TEvUnsubscribe());
        }
        TActorBootstrapped::PassAway();
    }

    void Handle(TPqControlPlaneEvents::TEvDescribeConsumerResult::TPtr& ev) {
        const auto& result = ev->Get()->Record;

        NYdb::NIssue::TIssues issues;
        NYdb::NIssue::IssuesFromMessage(result.GetIssues(), issues);
        const NYdb::TStatus status(static_cast<NYdb::EStatus>(result.GetStatus()), std::move(issues));
        if (!status.IsSuccess()) {
            Fail(TStringBuilder() << "Failed to describe consumer \"" << Connection.GetConsumerName() << "\" for topic \"" << Connection.GetTopicPath() << "\"", status);
            return;
        }

        for (const auto& partition : result.GetPartitions()) {
            Y_VALIDATE(PartitionsToRead.erase(partition.GetPartitionId()), "Unexpected partition while initializing consumer offsets: " << partition.GetPartitionId());
            Y_VALIDATE(partition.HasStartOffset() && partition.HasCommittedOffset(), "Missing partition statistics while initializing consumer offsets: " << partition.GetPartitionId());

            const auto offset = partition.GetStartOffset();
            if (offset >= partition.GetCommittedOffset()) {
                continue;
            }

            SRC_LOG_I("Rewind consumer \"" << Connection.GetConsumerName() << "\", topic \"" << Connection.GetTopicPath() << "\", partition " << partition.GetPartitionId() << " from " << partition.GetCommittedOffset() << " to " << offset);
            ++PendingRequests;

            TopicClient->CommitOffset(Connection.GetTopicPath(), partition.GetPartitionId(), Connection.GetConsumerName(), offset)
                .Subscribe([partitionId = partition.GetPartitionId(), actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId()](const auto& future) {
                    actorSystem->Send(selfId, new TEvRewindFinished(partitionId, future));
                });
        }

        Y_VALIDATE(PartitionsToRead.empty(), "Missing partitions while initializing consumer offsets");
        RequestFinished();
    }

    void Handle(TEvRewindFinished::TPtr& ev) {
        if (const auto& result = ev->Get()->Result.GetValue(); !result.IsSuccess()) {
            Fail(TStringBuilder() << "Failed to rewind consumer \"" << Connection.GetConsumerName() << "\" for topic \"" << Connection.GetTopicPath() << "\", partition " << ev->Get()->PartitionId, result);
            return;
        }

        RequestFinished();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        if (ControlPlaneSubscribed && ev->Get()->NodeId == ControlPlaneActorId.NodeId()) {
            Fail("PQ control-plane actor disconnected while initializing consumer offsets", NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, {}));
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        Fail("Failed to deliver consumer description request to PQ control-plane actor", NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, {}));
    }

    void RequestFinished() {
        Y_VALIDATE(PendingRequests, "Unexpected consumer initialization completion without pending requests");
        if (!--PendingRequests) {
            Finish({});
        }
    }

    void Fail(const TString& message, const NYdb::TStatus& status) {
        SRC_LOG_E(message << ": " << status.GetIssues().ToOneLineString());

        TIssue issue(message);
        for (const auto& subIssue : status.GetIssues()) {
            issue.AddSubIssue(new TIssue(NYdb::NAdapters::ToYqlIssue(subIssue)));
        }

        Finish(TIssues({issue}));
    }

    void Finish(TIssues issues) {
        Send(ReaderId, new NActors::TEvents::TEvInvokeResult(
            [complete = std::move(Complete), issues = std::move(issues)](auto&, const auto&) mutable {
                complete(std::move(issues));
            }, [] {})
        );
        PassAway();
    }

    const NActors::TActorId ReaderId;
    const NActors::TActorId ControlPlaneActorId;
    const TConnection Connection;
    const ITopicClient::TPtr TopicClient;
    THashSet<ui64> PartitionsToRead;
    const TString LogPrefix;
    TComplete Complete;
    ui64 PendingRequests = 1;
    bool ControlPlaneSubscribed = false;
};

constexpr ui32 STATE_VERSION = 1;

} // anonymous namespace

TDqPqReadActorBase::TDqPqReadActorBase(
    ui64 inputIndex,
    ui64 taskId,
    NActors::TActorId selfId,
    const TTxId& txId,
    NPq::NProto::TDqPqTopicSource&& sourceParams,
    TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
    const NActors::TActorId& computeActorId,
    const NActors::TActorId& controlPlaneActorId)
    : InputIndex(inputIndex)
    , TxId(txId)
    , SourceParams(std::move(sourceParams))
    , StartingMessageTimestamp(InitStartingMessageTimestamp(SourceParams))
    , LogPrefix(TStringBuilder() << "SelfId: " << selfId << ", TxId: " << txId << ", task: " << taskId << ". PQ source. ")
    , ReadParams(std::move(readParams))
    , ComputeActorId(computeActorId)
    , TaskId(taskId)
    , ControlPlaneActorId(controlPlaneActorId)
{}

void TDqPqReadActorBase::InitConsumerOffsets(
    const NActors::TActorId& selfId,
    const NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo& cluster,
    ITopicClient::TPtr topicClient,
    ui32 partitionsCount)
{
    if (!TControlPlaneInteractor::NeedsRewind(SourceParams, StartingMessageTimestamp)) {
        return;
    }

    THashSet<ui64> partitionsToRewind;
    for (const auto& readParams : ReadParams) {
        for (const auto& params : readParams.GetPartitioningParams()) {
            for (ui64 partitionId = params.GetEachTopicPartitionGroupId(); partitionId < partitionsCount; partitionId += params.GetDqPartitionsCount()) {
                const auto it = Partitions.find(TPartitionKey{TString(cluster.Name), partitionId});

                // Keep checkpoint offsets unchanged. The read session must fail if the
                // server has already committed beyond them, rather than hide possible data loss.
                if (it == Partitions.end() || !it->second.Offset) {
                    partitionsToRewind.insert(partitionId);
                }
            }
        }
    }

    if (partitionsToRewind.empty()) {
        return;
    }

    std::string topicPath = SourceParams.GetTopicPath();
    cluster.AdjustTopicPath(topicPath);
    NPq::NProto::TEvDescribeConsumer::TConnection connection;
    connection.SetEndpoint(cluster.Name.empty() ? SourceParams.GetEndpoint() : TString(cluster.Endpoint));
    connection.SetDatabase(cluster.Name.empty() ? SourceParams.GetDatabase() : TString(cluster.Path));
    connection.SetTopicPath(TString(topicPath));
    connection.SetConsumerName(SourceParams.GetConsumerName());
    connection.SetUseSsl(SourceParams.GetUseSsl());
    connection.SetAddBearerToToken(SourceParams.GetAddBearerToToken());
    connection.SetTokenName(SourceParams.GetToken().GetName());

    auto complete = [this](TIssues issues) {
        if (!issues.Empty()) {
            ConsumerOffsetsRewindFailed = true;
            NActors::TActivationContext::Send(new NActors::IEventHandle(ComputeActorId, NActors::TActivationContext::AsActorContext().SelfID, new TEvAsyncInputError(InputIndex, std::move(issues), NDqProto::StatusIds::EXTERNAL_ERROR)));
            StopConsumerOffsetInitialization();
            return;
        }

        if (ConsumerOffsetsInitialized()) {
            OnConsumerOffsetsInitialized();
        }
    };

    ControlPlaneInteractors.insert(NActors::TActivationContext::Register(new TControlPlaneInteractor(
        selfId, ControlPlaneActorId, std::move(connection), std::move(topicClient),
        std::move(partitionsToRewind), LogPrefix, std::move(complete)
    )));
}

void TDqPqReadActorBase::HandleConsumerOffsets(NActors::TEvents::TEvInvokeResult::TPtr& ev) {
    if (ControlPlaneInteractors.erase(ev->Sender) && !ConsumerOffsetsRewindFailed) {
        ev->Get()->Process(NActors::TActivationContext::AsActorContext());
    }
}

void TDqPqReadActorBase::StopConsumerOffsetInitialization() {
    for (const auto& actorId : ControlPlaneInteractors) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(actorId, NActors::TActivationContext::AsActorContext().SelfID, new NActors::TEvents::TEvPoison()));
    }
    ControlPlaneInteractors.clear();
}

bool TDqPqReadActorBase::ConsumerOffsetsInitialized() const {
    return ControlPlaneInteractors.empty() && !ConsumerOffsetsRewindFailed;
}

void TDqPqReadActorBase::SaveState(const NDqProto::TCheckpoint& /*checkpoint*/, TSourceState& state) {
    NPq::NProto::TDqPqTopicSourceState stateProto;

    NPq::NProto::TDqPqTopicSourceState::TTopicDescription* topic = stateProto.AddTopics();
    topic->SetDatabaseId(SourceParams.GetDatabaseId());
    topic->SetEndpoint(SourceParams.GetEndpoint());
    topic->SetDatabase(SourceParams.GetDatabase());
    topic->SetTopicPath(SourceParams.GetTopicPath());

    for (const auto& [clusterAndPartition, info] : Partitions) {
        if (!info.Offset) {
            continue;
        }
        const auto& [cluster, partition] = clusterAndPartition;
        NPq::NProto::TDqPqTopicSourceState::TPartitionReadState* partitionState = stateProto.AddPartitions();
        partitionState->SetTopicIndex(0); // Now we are supporting only one topic per source.
        partitionState->SetCluster(cluster);
        partitionState->SetPartition(partition);
        partitionState->SetOffset(*info.Offset);
    }

    SRC_LOG_D("SessionId: " << GetSessionId() << " SaveState, offsets: " << LogPartitionToOffset());

    stateProto.SetStartingMessageTimestampMs(StartingMessageTimestamp.MilliSeconds());
    stateProto.SetIngressBytes(IngressStats.Bytes);

    TString stateBlob;
    YQL_ENSURE(stateProto.SerializeToString(&stateBlob));

    state.Data.emplace_back(stateBlob, STATE_VERSION);
}

void TDqPqReadActorBase::LoadState(const TSourceState& state) {
    InitWatermarkTracker();

    TInstant minStartingMessageTs = state.DataSize() ? TInstant::Max() : StartingMessageTimestamp;
    ui64 ingressBytes = 0;
    for (const auto& data : state.Data) {
        if (data.Version != STATE_VERSION) {
            ythrow yexception() << "Invalid state version, expected " << STATE_VERSION << ", actual " << data.Version;
        }

        NPq::NProto::TDqPqTopicSourceState stateProto;
        YQL_ENSURE(stateProto.ParseFromString(data.Blob), "Serialized state is corrupted");
        YQL_ENSURE(stateProto.TopicsSize() == 1, "One topic per source is expected");

        Partitions.reserve(Partitions.size() + stateProto.PartitionsSize());
        for (const auto& partitionProto : stateProto.GetPartitions()) {
            auto& offset = Partitions[TPartitionKey{partitionProto.GetCluster(), partitionProto.GetPartition()}].Offset;
            if (offset) {
                offset = Min(*offset, partitionProto.GetOffset());
            } else {
                offset = partitionProto.GetOffset();
            }
        }

        minStartingMessageTs = Min(minStartingMessageTs, TInstant::MilliSeconds(stateProto.GetStartingMessageTimestampMs()));
        ingressBytes += stateProto.GetIngressBytes();
    }

    SRC_LOG_D("SessionId: " << GetSessionId() << " StartingMessageTs " << minStartingMessageTs << " Restoring offset: " << LogPartitionToOffset());

    StartingMessageTimestamp = minStartingMessageTs;
    IngressStats.Bytes += ingressBytes;
    IngressStats.Chunks++;
}

ui64 TDqPqReadActorBase::GetInputIndex() const {
    return InputIndex;
}

const NYql::NDq::TDqAsyncStats& TDqPqReadActorBase::GetIngressStats() const {
    return IngressStats;
}

TString TDqPqReadActorBase::GetSessionId() const {
    return "empty";
}

void TDqPqReadActorBase::InitWatermarkTracker(TDuration lateArrivalDelay, TDuration idleTimeout, const ::NMonitoring::TDynamicCounterPtr& counters) {
    const auto granularity = TDuration::MicroSeconds(SourceParams.GetWatermarks().GetGranularityUs());
    SRC_LOG_D("SessionId: " << GetSessionId() << " Watermarks enabled: " << SourceParams.GetWatermarks().GetEnabled() << " granularity: " << granularity
        << " late arrival delay: " << lateArrivalDelay
        << " idle: " << SourceParams.GetWatermarks().GetIdlePartitionsEnabled()
        << " idle timeout: " << idleTimeout
    );

    if (!SourceParams.GetWatermarks().GetEnabled()) {
        return;
    }

    WatermarkTracker.ConstructInPlace(
        granularity,
        SourceParams.GetWatermarks().GetIdlePartitionsEnabled(),
        lateArrivalDelay,
        idleTimeout,
        LogPrefix,
        counters
    );
}

void TDqPqReadActorBase::MaybeSchedulePartitionIdlenessCheck(TInstant systemTime) {
    Y_DEBUG_ABORT_UNLESS(WatermarkTracker);
    if (const auto nextIdleCheckAt = WatermarkTracker->PrepareIdlenessCheck(systemTime)) {
        SRC_LOG_T("Next idleness check scheduled at " << *nextIdleCheckAt);
        SchedulePartitionIdlenessCheck(*nextIdleCheckAt);
    }
}

TString TDqPqReadActorBase::LogPartitionToOffset() const {
    TStringBuilder str;
    for (const auto& [clusterAndPartition, info] : Partitions) {
        str << "{" << clusterAndPartition.Cluster << ":" << clusterAndPartition.PartitionId << "," << info.Offset << "},";
    }
    return str;
}

} // namespace NYql::NDq::NInternal
