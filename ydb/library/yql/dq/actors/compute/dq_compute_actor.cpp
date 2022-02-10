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
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_TASKS_RUNNER);
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

    TDqComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask&& task,
        IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory, 
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const TTaskRunnerFactory& taskRunnerFactory)
        : TBase(executerId, txId, std::move(task), std::move(sourceActorFactory), std::move(sinkActorFactory), settings, memoryLimits)
        , TaskRunnerFactory(taskRunnerFactory)
    {}

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        auto taskRunner = TaskRunnerFactory(GetTask(), logger);
        SetTaskRunner(taskRunner);
        PrepareTaskRunner();

        ContinueExecute();
    }

    void FillExtraStats(NDqProto::TDqComputeActorStats* /* dst */, bool /* last */) {
    }

private:
    const TTaskRunnerFactory TaskRunnerFactory;
};


IActor* CreateDqComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask&& task,
    IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory, 
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, const TTaskRunnerFactory& taskRunnerFactory) 
{
    return new TDqComputeActor(executerId, txId, std::move(task), std::move(sourceActorFactory),
        std::move(sinkActorFactory), settings, memoryLimits, taskRunnerFactory);
}

} // namespace NDq
} // namespace NYql
