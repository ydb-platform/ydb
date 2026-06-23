#include "dq_sync_compute_actor_base.h"
#include "dq_compute_actor.h"
#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql {
namespace NDq {

using namespace NActors;

namespace {

TDqExecutionSettings ExecutionSettings;

} // anonymous namespace

const TDqExecutionSettings& GetDqExecutionSettings() {
    return ExecutionSettings;
}

TDqExecutionSettings& GetDqExecutionSettingsForTests() {
    return ExecutionSettings;
}

class TDqComputeActor : public TDqSyncComputeActorBase<TDqComputeActor> {
    using TBase = TDqSyncComputeActorBase<TDqComputeActor>;

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR";

    TDqComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask* task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const TTaskRunnerFactory& taskRunnerFactory,
        ::NMonitoring::TDynamicCounterPtr taskCounters)
        : TBase(executerId, txId, task, std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, false, false, taskCounters)
        , TaskRunnerFactory(taskRunnerFactory)
    {
        InitializeTask();
    }

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        MemoryQuota = InitMemoryQuota();

        TLogFunc logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (NActors::NLog::EPrio priority, const TString& message) {
            LOG_LOG_S(*actorSystem, static_cast<NActors::NLog::EPriority>(priority), NKikimrServices::KQP_COMPUTE, "TxId: " << txId
                << ", task: " << taskId << ": " << message);
        };

        auto taskRunner = TaskRunnerFactory(GetAllocatorPtr(), Task, RuntimeSettings.StatsMode, logger);
        SetTaskRunner(taskRunner);
        auto selfId = this->SelfId();
        auto wakeupCallback = [actorSystem, selfId]() {
            actorSystem->Send(selfId, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback});
        };
        auto errorCallback = [actorSystem, selfId](const TString& error) {
            actorSystem->Send(selfId, new TEvDq::TEvAbortExecution(NYql::NDqProto::StatusIds::INTERNAL_ERROR, error));
        };
        TDqTaskRunnerExecutionContext execCtx(TxId, std::move(wakeupCallback), std::move(errorCallback));
        PrepareTaskRunner(execCtx);

        ContinueExecute(EResumeSource::CABootstrap);
    }

    void FillExtraStats(NDqProto::TDqComputeActorStats* /* dst */, bool /* last */) {
    }

private:
    const TTaskRunnerFactory TaskRunnerFactory;
};


IActor* CreateDqComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask* task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const TTaskRunnerFactory& taskRunnerFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters)
{
    return new TDqComputeActor(executerId, txId, task, std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, taskRunnerFactory, taskCounters);
}

} // namespace NDq
} // namespace NYql
