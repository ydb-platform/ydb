#include "kqp_tasks_runner.h"

#include "kqp_runtime_impl.h"

#include <ydb/core/kqp/common/kqp_resolve.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

#include <util/generic/vector.h>

namespace NKikimr {

namespace NMiniKQL {
class TKqpScanComputeContext;
} // namespace NMiniKQL

namespace NKqp {

using namespace NMiniKQL;
using namespace NYql;
using namespace NDq;

IDqOutputConsumer::TPtr KqpBuildOutputConsumer(const NDqProto::TTaskOutput& outputDesc, const TType* type,
    NUdf::IApplyContext* applyCtx, const TTypeEnvironment& typeEnv, TVector<IDqOutput::TPtr>&& outputs)
{
    switch (outputDesc.GetTypeCase()) {
        case NDqProto::TTaskOutput::kRangePartition: {
            TVector<NScheme::TTypeInfo> keyColumnTypeInfos;
            keyColumnTypeInfos.reserve(outputDesc.GetRangePartition().GetKeyColumns().size());
            TVector<TType*> keyColumnTypes;
            TVector<ui32> keyColumnIndices;
            GetColumnsInfo(type, outputDesc.GetRangePartition().GetKeyColumns(), keyColumnTypes, keyColumnIndices);
            YQL_ENSURE(!keyColumnTypes.empty());
            std::transform(keyColumnTypes.begin(), keyColumnTypes.end(), back_inserter(keyColumnTypeInfos), [](const auto& tyPtr) {
                // TODO: support pg types
                YQL_ENSURE(tyPtr->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data);
                auto dataTypeId = static_cast<NKikimr::NMiniKQL::TDataType&>(*tyPtr).GetSchemeType();
                return NScheme::TTypeInfo((NScheme::TTypeId)dataTypeId);
            });

            TVector<TKqpRangePartition> partitions;
            partitions.reserve(outputDesc.GetRangePartition().PartitionsSize());

            for (auto& partitionDesc : outputDesc.GetRangePartition().GetPartitions()) {
                TKqpRangePartition partition;
                partition.ChannelId = partitionDesc.GetChannelId();
                partition.Range.EndKeyPrefix = TSerializedCellVec(partitionDesc.GetEndKeyPrefix());
                partition.Range.IsInclusive = partitionDesc.GetIsInclusive();
                partition.Range.IsPoint = partitionDesc.GetIsPoint();

                partitions.emplace_back(std::move(partition));
            }

            return CreateOutputRangePartitionConsumer(std::move(outputs), std::move(partitions),
                std::move(keyColumnTypeInfos), std::move(keyColumnIndices), typeEnv);
        }

        case NDqProto::TTaskOutput::kEffects: {
            return CreateKqpApplyEffectsConsumer(applyCtx);
        }

        default: {
            return DqBuildOutputConsumer(outputDesc, type, typeEnv, std::move(outputs));
        }
    }
}

TIntrusivePtr<IDqTaskRunner> CreateKqpTaskRunner(const TDqTaskRunnerContext& execCtx,
    const TDqTaskRunnerSettings& settings, const TLogFunc& logFunc)
{
    return MakeDqTaskRunner(execCtx, settings, logFunc);
}


TKqpTasksRunner::TKqpTasksRunner(google::protobuf::RepeatedPtrField<NDqProto::TDqTask>&& tasks,
    const TDqTaskRunnerContext& execCtx, const TDqTaskRunnerSettings& settings, const TLogFunc& logFunc)
    : LogFunc(logFunc)
    , Alloc(execCtx.Alloc)
{
    YQL_ENSURE(execCtx.Alloc);
    YQL_ENSURE(execCtx.TypeEnv);

    ApplyCtx = dynamic_cast<NMiniKQL::TKqpDatashardApplyContext *>(execCtx.ApplyCtx);
    YQL_ENSURE(ApplyCtx);
    ComputeCtx = dynamic_cast<NMiniKQL::TKqpComputeContextBase*>(execCtx.ComputeCtx);
    YQL_ENSURE(ComputeCtx);

    auto guard = execCtx.TypeEnv->BindAllocator();
    try {
        for (auto&& task : tasks) {
            ui64 taskId = task.GetId();
            auto runner = CreateKqpTaskRunner(execCtx, settings, logFunc);
            if (auto* stats = runner->GetStats()) {
                Stats.emplace(taskId, stats);
            }
            TaskRunners.emplace(taskId, std::move(runner));
            Tasks.emplace(taskId, &task);
        }
    } catch (const TMemoryLimitExceededException&) {
        TaskRunners.clear();
        Tasks.clear();
        throw;
    }
}

TKqpTasksRunner::~TKqpTasksRunner() {
    for (auto& [_, runner] : TaskRunners) {
        auto guard = runner->BindAllocator();
        runner.Reset();
    }
}

void TKqpTasksRunner::Prepare(const TDqTaskRunnerMemoryLimits& memoryLimits, const IDqTaskRunnerExecutionContext& execCtx)
{
    if (State >= EState::Prepared) {
        return;
    }
    YQL_ENSURE(State == EState::Initial, "" << (int) State);

    for (auto& [taskId, taskRunner] : TaskRunners) {
        ComputeCtx->SetCurrentTaskId(taskId);
        auto it = Tasks.find(taskId);
        Y_VERIFY(it != Tasks.end());
        taskRunner->Prepare(it->second, memoryLimits, execCtx);
    }

    ComputeCtx->SetCurrentTaskId(std::numeric_limits<ui64>::max());

    State = EState::Prepared;
}

ERunStatus TKqpTasksRunner::Run(bool applyEffects) {
    YQL_ENSURE(State >= EState::Prepared, "" << (int) State);
    State = EState::Running;

    bool hasPendingInputTasks = false;
    bool hasPendingOutputTasks = false;

    // for per-task statistics in KqpUpsertRows and KqpDeleteRows computation nodes
    auto dsCtx = dynamic_cast<TKqpDatashardComputeContext*>(ComputeCtx);
    for (auto& [taskId, task] : TaskRunners) {
        if (Y_UNLIKELY(LogFunc)) {
            LogFunc(TStringBuilder() << "running task: " << taskId);
        }

        if (!applyEffects && task->HasEffects()) {
            // TODO: effects-only task?
            continue;
        }

        if (dsCtx) {
            ApplyCtx->TaskTableStats = &dsCtx->GetTaskCounters(taskId);
        }
        auto status = task->Run();

        switch (status) {
            case ERunStatus::PendingInput:
            case ERunStatus::PendingOutput: {
                if (applyEffects && task->HasEffects()) {
                    // Keep effects order between tasks, can't process next task before all
                    // effects from the current one are applied.
                    if (Y_UNLIKELY(LogFunc)) {
                        LogFunc(TStringBuilder() << "task: " << taskId << " execution status: " << status
                            << ", stop tasks execution to preserve effects order between tasks");
                    }
                    return status;
                }
                hasPendingInputTasks = status == ERunStatus::PendingInput;
                hasPendingOutputTasks = status == ERunStatus::PendingOutput;
                break;
            }
            case ERunStatus::Finished:
                break;
        }

        if (Y_UNLIKELY(LogFunc)) {
            LogFunc(TStringBuilder() << "task: " << taskId << " execution status: " << status);
        }
    }

    if (hasPendingOutputTasks) {
        return ERunStatus::PendingOutput;
    }
    if (hasPendingInputTasks) {
        return ERunStatus::PendingInput;
    }

    return ERunStatus::Finished;
}

std::pair<bool, bool> TKqpTasksRunner::TransferData(ui64 fromTask, ui64 fromChannelId, ui64 toTask, ui64 toChannelId) {
    auto src = GetOutputChannel(fromTask, fromChannelId);
    auto dst = GetInputChannel(toTask, toChannelId);

    bool transferred = false;
    bool finished = false;

    // todo: transfer data as-is from input- to output- channel (KIKIMR-10658)
    for (;;) {
        NDq::TDqSerializedBatch data;
        if (!src->Pop(data)) {
            break;
        }
        transferred = true;
        dst->Push(std::move(data));
    }

    if (src->IsFinished()) {
        finished = true;
        dst->Finish();
    }

    return std::make_pair(transferred, finished);
}

IDqTaskRunner& TKqpTasksRunner::GetTaskRunner(ui64 taskId) {
    auto task = TaskRunners.FindPtr(taskId);
    YQL_ENSURE(task);

    return **task;
}

const IDqTaskRunner& TKqpTasksRunner::GetTaskRunner(ui64 taskId) const {
    auto task = TaskRunners.FindPtr(taskId);
    YQL_ENSURE(task);

    return **task;
}

const NYql::NDq::TDqTaskSettings& TKqpTasksRunner::GetTask(ui64 taskId) const {
    return Tasks.at(taskId);
}

TGuard<NMiniKQL::TScopedAlloc> TKqpTasksRunner::BindAllocator(TMaybe<ui64> memoryLimit) {
    if (memoryLimit) {
        Alloc->SetLimit(*memoryLimit);
    }
    return TGuard(*Alloc);
}

TIntrusivePtr<TKqpTasksRunner> CreateKqpTasksRunner(google::protobuf::RepeatedPtrField<NDqProto::TDqTask>&& tasks,
    const TDqTaskRunnerContext& execCtx, const TDqTaskRunnerSettings& settings, const TLogFunc& logFunc)
{
    return new TKqpTasksRunner(std::move(tasks), execCtx, settings, logFunc);
}

} // namespace NKqp
} // namespace NKikimr
