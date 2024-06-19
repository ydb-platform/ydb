#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>

#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>

#include <util/generic/size_literals.h>

#include "compute_actor.h"

namespace NYql {

using namespace NActors;
using namespace NKikimr;
using namespace NDqs;

IActor* CreateComputeActor(
    const TLocalWorkerManagerOptions& options,
    NDq::IMemoryQuotaManager::TPtr memoryQuotaManager,
    const TActorId& executerId,
    const TString& operationId,
    NYql::NDqProto::TDqTask* task,
    const TString& computeActorType,
    const NDq::NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    NDqProto::EDqStatsMode statsMode)
{
    auto memoryLimits = NDq::TComputeMemoryLimits();
    memoryLimits.ChannelBufferSize = 1000000;
    // light == heavy since we allow extra allocation
    memoryLimits.MkqlLightProgramMemoryLimit = options.MkqlInitialMemoryLimit;
    memoryLimits.MkqlHeavyProgramMemoryLimit = options.MkqlInitialMemoryLimit;
    memoryLimits.MkqlProgramHardMemoryLimit = options.MkqlProgramHardMemoryLimit;
    memoryLimits.MemoryQuotaManager = memoryQuotaManager;
    // min alloc size == min free size to simplify api
    memoryLimits.MinMemAllocSize = options.MkqlMinAllocSize;
    memoryLimits.MinMemFreeSize = options.MkqlMinAllocSize;

    auto computeRuntimeSettings = NDq::TComputeRuntimeSettings();
    computeRuntimeSettings.ExtraMemoryAllocationPool = 3;
    computeRuntimeSettings.FailOnUndelivery = false;
    computeRuntimeSettings.StatsMode = (statsMode != NDqProto::DQ_STATS_MODE_UNSPECIFIED) ? statsMode : NDqProto::DQ_STATS_MODE_FULL;
    computeRuntimeSettings.AsyncInputPushLimit = 64_MB;

    // clear fake actorids
    for (auto& input : *task->MutableInputs()) {
        for (auto& channel : *input.MutableChannels()) {
            channel.MutableSrcEndpoint()->ClearActorId();
            channel.MutableDstEndpoint()->ClearActorId();
        }
    }
    for (auto& output : *task->MutableOutputs()) {
        for (auto& channel : *output.MutableChannels()) {
            channel.MutableSrcEndpoint()->ClearActorId();
            channel.MutableDstEndpoint()->ClearActorId();
        }
    }

    auto taskRunnerFactory = [factory = options.Factory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const NDq::TLogFunc& logger) {
        Y_UNUSED(logger);
        return factory->Get(alloc, task, statsMode, {});
    };

    if (computeActorType.empty() || computeActorType == "old" || computeActorType == "sync") {
        return NYql::NDq::CreateDqComputeActor(
            executerId,
            operationId,
            task,
            options.AsyncIoFactory,
            options.FunctionRegistry,
            computeRuntimeSettings,
            memoryLimits,
            taskRunnerFactory,
            taskCounters);
    } else {
        return NYql::NDq::CreateDqAsyncComputeActor(
            executerId,
            operationId,
            task,
            options.AsyncIoFactory,
            options.FunctionRegistry,
            computeRuntimeSettings,
            memoryLimits,
            taskRunnerActorFactory,
            taskCounters,
            options.QuoterServiceActorId,
            options.ComputeActorOwnsCounters);
    }
}

} /* namespace NYql */
