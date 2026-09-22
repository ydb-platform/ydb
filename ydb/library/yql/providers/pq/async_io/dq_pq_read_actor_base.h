#pragma once

#include <ydb/library/actors/core/invoke.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/runtime/streaming/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>

namespace NYql::NDq::NInternal {

class TDqPqReadActorBase : public IDqComputeActorAsyncInput {
protected:
    struct TPartitionInfo {
        std::optional<ui64> Offset;             // offset of next event.
        std::optional<ui64> EndOffset;          // end offset in topic on start.
        TMaybe<TInstant> EndWriteTime;          // from predicate.
        TInstant LastMessageWriteTime;

        bool IsFinishedInTableMode() {
            if (!EndOffset                      // Not connected yet.
                && !EndWriteTime) {
                return false;
            }
            bool endByOffset =
                EndOffset
                && (*EndOffset == 0             // No data in partition on start.
                    || (Offset && *EndOffset <= *Offset));
            if (endByOffset) {
                return true;
            }
            return EndWriteTime && *EndWriteTime <= LastMessageWriteTime;
        }
    };

    const ui64 InputIndex = 0;
    THashMap<TPartitionKey, TPartitionInfo> Partitions;
    const TTxId TxId;
    NPq::NProto::TDqPqTopicSource SourceParams;
    TDqAsyncStats IngressStats;
    TInstant StartingMessageTimestamp;
    TString LogPrefix;
    TVector<NPq::NProto::TDqReadTaskParams> ReadParams;
    const NActors::TActorId ComputeActorId;
    const ui64 TaskId = 0;
    TMaybe<TDqSourceWatermarkTracker<TPartitionKey>> WatermarkTracker;
    // << Initialized when watermark tracking is enabled

public:
    TDqPqReadActorBase(
        ui64 inputIndex,
        ui64 taskId,
        NActors::TActorId selfId,
        const TTxId& txId,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& controlPlaneActorId);

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override;

    void LoadState(const TSourceState& state) override;

    ui64 GetInputIndex() const override;

    const TDqAsyncStats& GetIngressStats() const override;

protected:
    virtual void SchedulePartitionIdlenessCheck(TInstant) = 0;

    virtual void InitWatermarkTracker() = 0;

    virtual TString GetSessionId() const;

    void InitWatermarkTracker(TDuration, TDuration, const ::NMonitoring::TDynamicCounterPtr& counters = {});

    void MaybeSchedulePartitionIdlenessCheck(TInstant systemTime);

    void HandleConsumerOffsets(NActors::TEvents::TEvInvokeResult::TPtr& ev);

    void StopConsumerOffsetInitialization();

    void InitConsumerOffsets(
        const NActors::TActorId& selfId,
        const NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo& cluster,
        ITopicClient::TPtr topicClient,
        ui32 partitionsCount);

    bool ConsumerOffsetsInitialized() const;

    virtual void OnConsumerOffsetsInitialized() = 0;

private:
    const NActors::TActorId ControlPlaneActorId;
    THashSet<NActors::TActorId> ControlPlaneInteractors;
    bool ConsumerOffsetsRewindFailed = false;

    TString LogPartitionToOffset() const;
};

} // namespace NYql::NDq
