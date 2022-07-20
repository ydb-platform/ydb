#pragma once

#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NYql {

NActors::IActor* CreateComputeActor(
    const NYql::NDqs::TLocalWorkerManagerOptions& options,
    NDq::TAllocateMemoryCallback allocateMemoryFn,
    NDq::TFreeMemoryCallback freeMemoryFn,
    const NActors::TActorId& executerId,
    const TString& operationId,
    NYql::NDqProto::TDqTask&& task,
    const TString& computeActorType,
    const NDq::NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters = nullptr);

} // namespace NYql
