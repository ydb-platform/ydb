#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>

#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>
#include <ydb/library/yql/providers/dq/task_runner/task_runner_invoker.h>

#include <ydb/library/yql/providers/dq/task_runner_actor/task_runner_actor.h>

#include <ydb/library/actors/core/actorid.h>

namespace NYql {

struct TWorkerRuntimeData;

}

namespace NYql::NDqs {

    struct TLocalWorkerManagerOptions {
        TWorkerManagerCounters Counters;
        NTaskRunnerProxy::IProxyFactory::TPtr Factory;
        NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
        const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
        TWorkerRuntimeData* RuntimeData = nullptr;
        TTaskRunnerInvokerFactory::TPtr TaskRunnerInvokerFactory;
        NDq::NTaskRunnerActor::ITaskRunnerActorFactory::TPtr TaskRunnerActorFactory;
        THashMap<TString, TString> ClusterNamesMapping;

        ui64 MkqlInitialMemoryLimit = 8_GB;
        ui64 MkqlTotalMemoryLimit = 0;
        ui64 MkqlMinAllocSize = 30_MB;
        ui64 MkqlProgramHardMemoryLimit = 0;

        bool CanUseComputeActor = true;
        NActors::TActorId QuoterServiceActorId;
        bool ComputeActorOwnsCounters = false;
        bool DropTaskCountersOnFinish = true;
    };

    NActors::IActor* CreateLocalWorkerManager(const TLocalWorkerManagerOptions& options);

} // namespace NYql
