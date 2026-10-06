#pragma once
#include <ydb/library/yql/providers/common/message_stream/async_io/read_actor.h>

#include <ydb/library/actors/core/invoke.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>

namespace NYql::NDq::NInternal {

class TPqReadState : public NFq::NMessageStream::TMessageStreamReadState {
protected:
    using TPartitionInfo = NFq::NMessageStream::TPartitionProgress;

    const ui64 InputIndex = 0;
    const TTxId TxId;
    NPq::NProto::TDqPqTopicSource SourceParams;
    TString LogPrefix;
    TVector<NPq::NProto::TDqReadTaskParams> ReadParams;
    const NActors::TActorId ComputeActorId;
    const ui64 TaskId = 0;

public:
    virtual ~TPqReadState() = default;

    TPqReadState(
        ui64 inputIndex,
        ui64 taskId,
        NActors::TActorId selfId,
        const TTxId& txId,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& controlPlaneActorId);

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state);

    void LoadState(const TSourceState& state);

    ui64 GetInputIndex() const;

    const TDqAsyncStats& GetIngressStats() const;

protected:
    virtual TString GetSessionId() const;

    void HandleConsumerOffsets(NActors::TEvents::TEvInvokeResult::TPtr& ev);

    void StopConsumerOffsetInitialization();

    void InitConsumerOffsets(
        const NActors::TActorId& selfId,
        const NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo& cluster,
        std::shared_ptr<NFq::IMessageStreamClient> topicClient,
        ui32 partitionsCount);

    bool ConsumerOffsetsInitialized() const;

    virtual void OnConsumerOffsetsInitialized() = 0;

private:
    const NActors::TActorId ControlPlaneActorId;
    THashSet<NActors::TActorId> ControlPlaneInteractors;
    bool ConsumerOffsetsRewindFailed = false;

    TString LogPartitionToOffset() const;
};

// Existing row-dispatcher readers keep the async-input interface; the
// common direct reader uses TPqReadState without inheriting another input.
class TDqPqReadActorBase : public TPqReadState, public IDqComputeActorAsyncInput {
public:
    using TPqReadState::TPqReadState;
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
        TPqReadState::SaveState(checkpoint, state);
    }
    void LoadState(const TSourceState& state) override { TPqReadState::LoadState(state); }
    ui64 GetInputIndex() const override { return TPqReadState::GetInputIndex(); }
    const TDqAsyncStats& GetIngressStats() const override { return TPqReadState::GetIngressStats(); }
};

} // namespace NYql::NDq
