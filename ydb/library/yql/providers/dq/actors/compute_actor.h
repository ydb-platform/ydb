#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/local_worker_manager_options.h>

namespace NYql {

NActors::IActor* CreateComputeActor(
    const NYql::NDqs::TLocalWorkerManagerOptions& options,
    NDq::IMemoryQuotaManager::TPtr memoryQuotaManager,
    const NActors::TActorId& executerId,
    const TString& operationId,
    NDqProto::TDqTask* task,
    const TString& computeActorType,
    const NDq::NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    NDqProto::EDqStatsMode statsMode);

} // namespace NYql
