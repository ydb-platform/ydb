#include "kqp_tasks_validate.h"

using namespace NYql;

namespace NKikimr {
namespace NKqp {

namespace {

class TTasksValidator {
    using TGraphType = TKqpTasksGraph;
    using TStageType = TGraphType::TStageInfoType;
    using TTaskType = TGraphType::TTaskType;
    using TInputType = TGraphType::TTaskType::TInputType;
    using TOutputType = TGraphType::TTaskType::TOutputType;

public:
    TTasksValidator(const TGraphType& tasksGraph, const EExecType& execType, bool enableSpilling)
        : TasksGraph(tasksGraph)
        , ExecType(execType)
        , EnableSpilling(enableSpilling) {}

    void Validate() {
        for (auto& task : TasksGraph.GetTasks()) {
            ValidateTask(task);
        }
    }

private:
    void ValidateChannel(ui64 channelId) {
        auto& channel = TasksGraph.GetChannel(channelId);

        YQL_ENSURE(channel.SrcTask);
        auto& srcTask = TasksGraph.GetTask(channel.SrcTask);

        if (channel.DstTask) {
            auto& dstTask = TasksGraph.GetTask(channel.DstTask);

            auto& stageInfo = TasksGraph.GetStageInfo(dstTask.StageId);
            auto& dstStage = stageInfo.Meta.GetStage(stageInfo.Id);

            if (IsDataExec() && dstTask.Meta.ShardId && dstStage.SourcesSize() == 0) {
                YQL_ENSURE(srcTask.Meta.ShardId, "Invalid channel from non-shard task to shard task"
                    << ", channelId: " << channelId
                    << ", srcTaskId: " << channel.SrcTask
                    << ", dstTaskId: " << channel.DstTask);
            }
        }

        if (!EnableSpilling) {
            YQL_ENSURE(channel.InMemory, "With spilling off, all channels should be stored in memory only. "
                << "Not InMemory channelId: " << channelId);
        }
    }

    void ValidateInput(const TInputType& input) {
        for (ui64 channelId : input.Channels) {
            ValidateChannel(channelId);
        }
    }

    void ValidateOutput(const TOutputType& output) {
        for (ui64 channelId : output.Channels) {
            ValidateChannel(channelId);
        }
    }

    void ValidateTask(const TTaskType& task) {
        for (auto& input : task.Inputs) {
            ValidateInput(input);
        }

        for (auto& output : task.Outputs) {
            ValidateOutput(output);
        }

        if (task.Meta.Writes) {
            YQL_ENSURE(task.Outputs.size() == 1, "Read-write tasks should have single output.");
        }
    }

    bool IsDataExec() {
        return ExecType == EExecType::Data;
    }

private:
    const TGraphType& TasksGraph;
    EExecType ExecType;
    bool EnableSpilling;
};

} // namespace

bool ValidateTasks(const TKqpTasksGraph& tasksGraph, const EExecType& execType, bool enableSpilling, NYql::TIssue& issue) {
    try {
        TTasksValidator(tasksGraph, execType, enableSpilling).Validate();
        return true;
    } catch (const TYqlPanic& e) {
        issue = YqlIssue({}, TIssuesIds::DEFAULT_ERROR, e.what());
        return false;
    }
}

} // namespace NKqp
} // namespace NKikimr
