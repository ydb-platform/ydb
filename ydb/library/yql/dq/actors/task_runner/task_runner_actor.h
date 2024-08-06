#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>
#include <ydb/library/yql/dq/actors/task_runner/events.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDq {

namespace NTaskRunnerActor {

struct ITaskRunnerActor {
    struct ICallbacks {
        virtual ~ICallbacks() = default;
        virtual void SinkSend(
            ui64 index,
            NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
            TMaybe<NDqProto::TCheckpoint>&& checkpoint,
            i64 size,
            i64 checkpointSize,
            bool finished,
            bool changed) = 0;
    };

    virtual ~ITaskRunnerActor() = default;

    virtual void AsyncInputPush(
        ui64 cookie,
        ui64 index,
        NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
        i64 space,
        bool finish) = 0;

    virtual void PassAway() = 0;
};

struct ITaskRunnerActorFactory {
    using TPtr = std::shared_ptr<ITaskRunnerActorFactory>;

    virtual ~ITaskRunnerActorFactory() = default;

    virtual std::tuple<ITaskRunnerActor*, NActors::IActor*> Create(
        ITaskRunnerActor::ICallbacks* parent,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const TTxId& txId,
        ui64 taskId,
        THashSet<ui32>&& inputChannelsWithDisabledCheckpoints = {},
        THolder<NYql::NDq::TDqMemoryQuota>&& memoryQuota = {}, 
        ::NMonitoring::TDynamicCounterPtr taskCounters = {}) = 0;
};

ITaskRunnerActorFactory::TPtr CreateLocalTaskRunnerActorFactory(const TTaskRunnerFactory& factory);

} // namespace NTaskRunnerActor

} // namespace NYql::NDq
