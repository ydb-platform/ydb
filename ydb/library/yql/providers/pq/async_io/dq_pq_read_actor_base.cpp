#include "dq_pq_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <yql/essentials/utils/log/log.h>

#include <ydb/library/actors/core/log.h>

using namespace NYql::NDq::NInternal;

constexpr ui32 StateVersion = 1;

#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

void TDqPqReadActorBase::SaveState(const NDqProto::TCheckpoint& /*checkpoint*/, TSourceState& state) {
    NPq::NProto::TDqPqTopicSourceState stateProto;

    NPq::NProto::TDqPqTopicSourceState::TTopicDescription* topic = stateProto.AddTopics();
    topic->SetDatabaseId(SourceParams.GetDatabaseId());
    topic->SetEndpoint(SourceParams.GetEndpoint());
    topic->SetDatabase(SourceParams.GetDatabase());
    topic->SetTopicPath(SourceParams.GetTopicPath());

    TStringStream str;
    str << "SessionId: " << GetSessionId() << " SaveState, offsets: ";
    for (const auto& [clusterAndPartition, offset] : PartitionToOffset) {
        const auto& [cluster, partition] = clusterAndPartition;
        NPq::NProto::TDqPqTopicSourceState::TPartitionReadState* partitionState = stateProto.AddPartitions();
        partitionState->SetTopicIndex(0); // Now we are supporting only one topic per source.
        partitionState->SetCluster(cluster);
        partitionState->SetPartition(partition);
        partitionState->SetOffset(offset);
        str << "{" << partition << "," << offset << "},";
    }
    SRC_LOG_D(str.Str());

    stateProto.SetStartingMessageTimestampMs(StartingMessageTimestamp.MilliSeconds());
    stateProto.SetIngressBytes(IngressStats.Bytes);

    TString stateBlob;
    YQL_ENSURE(stateProto.SerializeToString(&stateBlob));

    state.Data.emplace_back(stateBlob, StateVersion);
}

void TDqPqReadActorBase::LoadState(const TSourceState& state) {
    TInstant minStartingMessageTs = state.DataSize() ? TInstant::Max() : StartingMessageTimestamp;
    ui64 ingressBytes = 0;
    for (const auto& data : state.Data) {
        if (data.Version != StateVersion) {
            ythrow yexception() << "Invalid state version, expected " << StateVersion << ", actual " << data.Version;
        }
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
    }
    TStringStream str;
    str << "SessionId: " << GetSessionId() << " Restoring offset: ";
    for (const auto& [key, value] : PartitionToOffset) {
        str << "{" << key.first << "," << key.second << "," << value << "},";
    }
    SRC_LOG_D(str.Str());
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