#include "kqp_task_graph.h"

namespace NKikimr::NKqp {

void SerializeSchedulingGraph(const NYql::NDq::TQueryTaskGraph& graph, NKikimrKqp::TTaskSchedulingGraph& result) {
    result.Clear();
    for (const auto& task : graph.Tasks) {
        auto* target = result.AddTasks();
        target->SetTaskId(task.TaskId);
        target->SetStageId(task.StageId);
        target->SetPlannedNodeId(task.PlannedNodeId);
        target->SetInputsCount(task.InputsCount);
        target->SetOutputsCount(task.OutputsCount);
        target->SetHasSource(task.HasSource);
        target->SetHasSink(task.HasSink);
        target->SetHasEffects(task.HasEffects);
    }
    for (const auto& channel : graph.Channels) {
        auto* target = result.AddChannels();
        target->SetChannelId(channel.ChannelId);
        target->SetSrcTaskId(channel.SrcTaskId);
        target->SetDstTaskId(channel.DstTaskId);
        target->SetSrcOutputIndex(channel.SrcOutputIndex);
        target->SetDstInputIndex(channel.DstInputIndex);
        target->SetRoutingKind(channel.RoutingKind);
    }
}

std::shared_ptr<const NYql::NDq::TQueryTaskGraph> DeserializeSchedulingGraph(const NKikimrKqp::TTaskSchedulingGraph& graph) {
    TVector<NYql::NDq::TTaskGraphNode> tasks;
    TVector<NYql::NDq::TTaskGraphChannel> channels;
    tasks.reserve(graph.TasksSize());
    channels.reserve(graph.ChannelsSize());
    for (const auto& task : graph.GetTasks()) {
        tasks.push_back({task.GetTaskId(), task.GetStageId(), task.GetPlannedNodeId(), task.GetInputsCount(),
                         task.GetOutputsCount(), task.GetHasSource(), task.GetHasSink(), task.GetHasEffects()});
    }
    for (const auto& channel : graph.GetChannels()) {
        channels.push_back({channel.GetChannelId(), channel.GetSrcTaskId(), channel.GetDstTaskId(),
                            channel.GetSrcOutputIndex(), channel.GetDstInputIndex(), channel.GetRoutingKind()});
    }
    return std::make_shared<const NYql::NDq::TQueryTaskGraph>(std::move(tasks), std::move(channels));
}

} // namespace NKikimr::NKqp
