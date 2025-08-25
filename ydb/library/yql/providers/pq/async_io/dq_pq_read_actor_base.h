#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/pq/common/pq_partition_key.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

namespace NYql::NDq::NInternal {

class TDqPqReadActorBase : public IDqComputeActorAsyncInput {
public:
    using TPartitionKey = ::NPq::TPartitionKey;

    const ui64 InputIndex;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    const TTxId TxId;
    NPq::NProto::TDqPqTopicSource SourceParams;
    TDqAsyncStats IngressStats;
    TInstant StartingMessageTimestamp;
    TString LogPrefix;
    TVector<NPq::NProto::TDqReadTaskParams> ReadParams;
    const NActors::TActorId ComputeActorId;
    ui64 TaskId;

    TDqPqReadActorBase(
        ui64 inputIndex,
        ui64 taskId,
        NActors::TActorId selfId,
        const TTxId& txId,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        TVector<NPq::NProto::TDqReadTaskParams>&& readParams,
        const NActors::TActorId& computeActorId);

public:
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override;
    void LoadState(const TSourceState& state) override;

    ui64 GetInputIndex() const override;
    const TDqAsyncStats& GetIngressStats() const override;

    virtual TString GetSessionId() const {
        return TString{"empty"};
    }
};

} // namespace NYql::NDq
