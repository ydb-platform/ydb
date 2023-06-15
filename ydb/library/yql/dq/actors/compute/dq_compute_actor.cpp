#include "dq_compute_actor_impl.h"
#include "dq_compute_actor.h"

#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql {
namespace NDq {

using namespace NActors;

namespace {
TDqExecutionSettings ExecutionSettings;

bool IsDebugLogEnabled(const TActorSystem* actorSystem) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_COMPUTE);
}

} // anonymous namespace

const TDqExecutionSettings& GetDqExecutionSettings() {
    return ExecutionSettings;
}

TDqExecutionSettings& GetDqExecutionSettingsForTests() {
    return ExecutionSettings;
}

class TDqComputeActor : public TDqComputeActorBase<TDqComputeActor> {
    using TBase = TDqComputeActorBase<TDqComputeActor>;

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR";

    TDqComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask* task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const TTaskRunnerFactory& taskRunnerFactory,
        ::NMonitoring::TDynamicCounterPtr taskCounters)
        : TBase(executerId, txId, task, std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, true, false, taskCounters)
        , TaskRunnerFactory(taskRunnerFactory)
    {}

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        auto taskRunner = TaskRunnerFactory(Task, logger);
        SetTaskRunner(taskRunner);
        PrepareTaskRunner();

        ContinueExecute();
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
