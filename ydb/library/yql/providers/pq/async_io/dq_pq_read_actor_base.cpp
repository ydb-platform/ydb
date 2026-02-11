#include "dq_pq_read_actor_base.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/interop/cast.h>

#define SRC_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq::NInternal {

namespace {

TInstant TrimToMillis(TInstant instant) {
    return TInstant::MilliSeconds(instant.MilliSeconds());
}

// StartingMessageTimestamp is serialized as milliseconds, so drop microseconds part to be consistent with storage
TInstant InitStartingMessageTimestamp(const NPq::NProto::StreamingDisposition& disposition) {
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

constexpr ui32 STATE_VERSION = 1;

} // anonymous namespace

TDqPqReadActorBase::TDqPqReadActorBase(
    ui64 inputIndex,
    ui64 taskId,
    NActors::TActorId selfId,
    const TTxId& txId,
    NPq::NProto::TDqPqTopicSource&& sourceParams,
    TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
    const NActors::TActorId& computeActorId)
    : InputIndex(inputIndex)
    , TxId(txId)
    , SourceParams(std::move(sourceParams))
    , StartingMessageTimestamp(InitStartingMessageTimestamp(SourceParams.GetDisposition()))
    , LogPrefix(TStringBuilder() << "SelfId: " << selfId << ", TxId: " << txId << ", task: " << taskId << ". PQ source. ")
    , ReadParams(std::move(readParams))
    , ComputeActorId(computeActorId)
    , TaskId(taskId)
{}

void TDqPqReadActorBase::SaveState(const NDqProto::TCheckpoint& /*checkpoint*/, TSourceState& state) {
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

        PartitionToOffset.reserve(PartitionToOffset.size() + stateProto.PartitionsSize());
        for (const auto& partitionProto : stateProto.GetPartitions()) {
            ui64& offset = PartitionToOffset[TPartitionKey{partitionProto.GetCluster(), partitionProto.GetPartition()}];
            if (offset) {
                offset = Min(offset, partitionProto.GetOffset());
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
    for (const auto& [clusterAndPartition, offset] : PartitionToOffset) {
        str << "{" << clusterAndPartition.Cluster << ":" << clusterAndPartition.PartitionId << "," << offset << "},";
    }
    return str;
}

} // namespace NYql::NDq::NInternal
