#include "dq_sync_compute_actor_base.h"
#include "dq_compute_actor.h"
#include "dq_task_runner_exec_ctx.h"

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

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

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
    void CheckRunStatus() override {
        // Standalone DQ readers may yield without an input actor to wake us.
        // Keep this retry out of TDqSyncComputeActorBase: KQP scan actors receive
        // data from their own fetchers and must wait for their notifications.
        // Retry an empty drain only when every output channel can accept data;
        // otherwise wait for the consumer's acknowledgement instead of busy-polling.
        auto& outputs = this->ProcessOutputsState;
        outputs.LastPopReturnedNoData = outputs.ChannelsReady && !outputs.DataWasSent;
        if (outputs.LastPopReturnedNoData) {
            for (const auto& [channelId, channel] : this->OutputChannelsMap) {
                if (channel.Finished) {
                    continue;
                }
                const bool canAcceptData = this->Channels
                    ? this->Channels->CanSendChannelData(channelId)
                        && this->Channels->HasFreeMemoryInChannel(channelId)
                    : channel.Channel->GetFillLevel() == EDqFillLevel::NoLimit;
                if (!canAcceptData) {
                    outputs.LastPopReturnedNoData = false;
                    break;
                }
            }
        }
        const i64 allowedOvercommit = TBase::AllowedChannelsOvercommit();
        const auto canDrainAsyncOutputs = [allowedOvercommit](const auto& asyncOutputs) {
            for (const auto& [_, output] : asyncOutputs) {
                if (!output.Finished && output.AsyncOutput->GetFreeSpace() + allowedOvercommit <= 0) {
                    return false;
                }
            }
            return true;
        };
        outputs.LastPopReturnedNoData = outputs.LastPopReturnedNoData
            && canDrainAsyncOutputs(this->SinksMap)
            && canDrainAsyncOutputs(this->OutputTransformsMap);
        TBase::CheckRunStatus();
    }

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
