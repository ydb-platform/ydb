#include "task_graph.h"

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <deque>

namespace NYql::NDq {

TQueryTaskGraph::TQueryTaskGraph(TVector<TTaskGraphNode> tasks, TVector<TTaskGraphChannel> channels, ui64 queryOrder)
    : Tasks(std::move(tasks)), Channels(std::move(channels)), QueryOrder(queryOrder)
{
    for (size_t i = 0; i < Tasks.size(); ++i) {
        Y_ENSURE(Tasks[i].TaskId && TaskIndex.emplace(Tasks[i].TaskId, i).second, "Duplicate or zero task ID");
    }
    THashSet<ui64> channelIds;
    THashSet<ui64> resultTasks;
    THashMap<ui64, TVector<ui64>> predecessors;
    for (const auto& channel : Channels) {
        Y_ENSURE(channel.ChannelId && channelIds.insert(channel.ChannelId).second, "Duplicate or zero channel ID");
        const auto* source = FindTask(channel.SrcTaskId);
        Y_ENSURE(source && channel.SrcOutputIndex < source->OutputsCount, "Invalid channel source");
        Outputs[channel.SrcTaskId].push_back(channel.ChannelId);
        if (channel.DstTaskId) {
            const auto* target = FindTask(channel.DstTaskId);
            Y_ENSURE(target && channel.DstInputIndex < target->InputsCount, "Invalid channel destination");
            Inputs[channel.DstTaskId].push_back(channel.ChannelId);
            predecessors[channel.DstTaskId].push_back(channel.SrcTaskId);
        } else {
            resultTasks.insert(channel.SrcTaskId);
        }
    }
    std::deque<ui64> pending;
    for (const auto& task : Tasks) {
        if (task.HasSink || task.HasEffects || resultTasks.contains(task.TaskId)) {
            ResultDistances.emplace(task.TaskId, 0);
            pending.push_back(task.TaskId);
        }
    }
    // Longest path to a result. A shortcut must not hide the chain the query
    // is still waiting on. A cycle cannot grow past the task count.
    while (!pending.empty()) {
        const auto id = pending.front();
        pending.pop_front();
        const auto base = ResultDistances.at(id);
        if (base >= Tasks.size()) {
            continue;
        }
        const auto next = base + 1;
        for (auto parent : predecessors[id]) {
            auto it = ResultDistances.find(parent);
            if (it == ResultDistances.end()) {
                ResultDistances.emplace(parent, next);
                pending.push_back(parent);
            } else if (next > it->second) {
                it->second = next;
                pending.push_back(parent);
            }
        }
    }
}

std::optional<ui32> TQueryTaskGraph::GetResultDistance(ui64 taskId) const {
    auto it = ResultDistances.find(taskId);
    return it == ResultDistances.end() ? std::nullopt : std::optional<ui32>(it->second);
}

const TTaskGraphNode* TQueryTaskGraph::FindTask(ui64 taskId) const {
    auto it = TaskIndex.find(taskId);
    return it == TaskIndex.end() ? nullptr : &Tasks[it->second];
}

const TVector<ui64>& TQueryTaskGraph::GetInputs(ui64 taskId) const {
    static const TVector<ui64> empty;
    auto it = Inputs.find(taskId);
    return it == Inputs.end() ? empty : it->second;
}

const TVector<ui64>& TQueryTaskGraph::GetOutputs(ui64 taskId) const {
    static const TVector<ui64> empty;
    auto it = Outputs.find(taskId);
    return it == Outputs.end() ? empty : it->second;
}

bool TQueryTaskGraph::IsResultTask(ui64 taskId) const {
    const auto* task = FindTask(taskId);
    if (!task) {
        return false;
    }
    if (task->HasSink || task->HasEffects) {
        return true;
    }
    for (const auto& channel : Channels) {
        if (channel.SrcTaskId == taskId && !channel.DstTaskId) {
            return true;
        }
    }
    return false;
}

} // namespace NYql::NDq
