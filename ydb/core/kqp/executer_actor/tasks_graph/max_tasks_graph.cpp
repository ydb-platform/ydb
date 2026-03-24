#include "max_tasks_graph.h"

#include <util/generic/yexception.h>

#include <numeric>
#include <unordered_map>

namespace NKikimr::NKqp {

TMaxTasksGraph::TMaxTasksGraph(size_t maxChannelsCount) : MaxChannelsCount(maxChannelsCount) {}

void TMaxTasksGraph::AddNodes(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot) {
    YQL_ENSURE(!resourcesSnapshot.empty());

    Nodes.reserve(resourcesSnapshot.size());
    Nodes.resize(resourcesSnapshot.size(), {MaxChannelsCount});

    size_t nodeIdx = 0;
    for (const auto& node : resourcesSnapshot) {
        Y_ENSURE(NodeIds.emplace(node.GetNodeId(), nodeIdx++).second);
    }
}

void TMaxTasksGraph::AddStage(const TStageId& stage, EStageType type, const std::list<TStageId>& inputs, std::optional<TStageId> copyInput) {
    TStage newStage;
    newStage.Type = type;
    newStage.Source = copyInput ? std::optional{StageIds.at(*copyInput)} : std::nullopt;

    Y_DEBUG_ABORT_UNLESS((newStage.Type == COPY) == newStage.Source.has_value());

    if (type == COPY) {
        const auto& prevStage = Stages.at(*newStage.Source);
        if (prevStage.Type == FIXED) {
            newStage.Type = FIXED;
        }
        if (prevStage.Source) {
            newStage.Source = prevStage.Source;
        }
    }

    TStageIdx newStageIdx = Stages.size();
    Stages.push_back(std::move(newStage));
    Y_ENSURE(StageIds.emplace(stage, newStageIdx).second);

    // inputs
    std::list<TStageIdx> newInputs;
    for (const auto& input : inputs) {
        newInputs.push_back(StageIds.at(input));
    }
    Inputs.push_back(newInputs);

    // outputs
    for (const auto input : newInputs) {
        Outputs.at(input).push_back(newStageIdx);
    }
    Outputs.push_back({});

    Tasks.push_back(TTasksPerNode(Nodes.size(), 0));
    TasksPerStage.push_back(0);

    Y_DEBUG_ABORT_UNLESS(Stages.size() == Inputs.size() && Inputs.size() == Tasks.size() && Inputs.size() == Outputs.size());
}

void TMaxTasksGraph::AddTasks(const TStageId& stage, TNodeId node, size_t tasksCount) {
    auto stageIdx = StageIds.at(stage);
    auto nodeIdx = NodeIds.find(node);
    if (nodeIdx == NodeIds.end()) {
        nodeIdx = NodeIds.emplace(node, NodeIds.size()).first;
    }

    Tasks.at(stageIdx).at(nodeIdx->second) += tasksCount;
    Nodes.at(nodeIdx->second).TasksCount += tasksCount;
    TasksPerStage.at(stageIdx) += tasksCount;
}

void TMaxTasksGraph::AddTasks(const TStageId& stage, size_t tasksCount) {
    auto stageIdx = StageIds.at(stage);
    TasksPerStage.at(stageIdx) += tasksCount;
    while (tasksCount--) {
        auto nodeIdx = Stages.at(stageIdx).RoundRobin;
        Tasks.at(stageIdx).at(nodeIdx)++;
        Nodes.at(nodeIdx).TasksCount++;
        if (++Stages.at(stageIdx).RoundRobin == Nodes.size()) {
            Stages.at(stageIdx).RoundRobin = 0;
        }
    }
}

void TMaxTasksGraph::Shrink() {
    if (Stages.empty() || Nodes.empty()) {
        return;
    }

    // TODO: verify that all stage groups have the same number of tasks.
    // TODO: verify there is no empty stages.

    double lo = 0.0;
    double hi = 1.0;

    if (IsFeasible(hi) || !IsFeasible(lo)) {
        return;
    }

    // Binary search the global scale coefficient
    const int maxIterations = 50;
    const double epsilon = 1 / double(std::ranges::max(Nodes, {}, &TNode::TasksCount).TasksCount);

    for (int i = 0; i < maxIterations && (hi - lo > epsilon); ++i) {
        double mid = (lo + hi) / 2.0;
        if (IsFeasible(mid)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    Tasks = std::move(LastFeasible);

    TasksPerStage.clear();
    TasksPerStage.resize(Stages.size(), 0);
    for (size_t stageIdx = 0; stageIdx < Tasks.size(); ++stageIdx) {
        for (auto tasksPerNode : Tasks.at(stageIdx)) {
            TasksPerStage.at(stageIdx) += tasksPerNode;
        }
    }
}

size_t TMaxTasksGraph::GetStageTasksCount(const TStageId& stage, TNodeId node) const {
    return Tasks.at(StageIds.at(stage)).at(NodeIds.at(node));
}

size_t TMaxTasksGraph::GetStageTasksCount(const TStageId& stage) const {
    return TasksPerStage.at(StageIds.at(stage));
}

bool TMaxTasksGraph::IsFeasible(double alpha) const {
    auto tasks = ComputeScaledTasks(alpha);

    for (TNodeId nodeId = 0; nodeId < Nodes.size(); ++nodeId) {
        uint64_t channels = CountChannelsOnNode(tasks, nodeId);
        if (channels > Nodes[nodeId].MaxChannelsCount) {
            return false;
        }
    }

    LastFeasible = std::move(tasks);

    return true;
}

std::vector<TMaxTasksGraph::TTasksPerNode> TMaxTasksGraph::ComputeScaledTasks(double alpha) const {
    if (alpha == 1.0) {
        return Tasks;
    }

    std::vector<TTasksPerNode> result(Stages.size());
    std::unordered_map<TStageIdx, size_t> scaledTotals;

    for (TStageIdx stageId = 0; stageId < Stages.size(); ++stageId) {
        TStageIdx root = Stages[stageId].Source.value_or(stageId);

        if (Stages[root].Type == FIXED) {
            result[stageId] = Tasks[stageId];
            continue;
        }

        auto rootIt = scaledTotals.find(root);
        if (rootIt == scaledTotals.end()) {
            auto rootTotal = std::accumulate(Tasks[root].begin(), Tasks[root].end(), size_t{0});
            rootIt = scaledTotals.emplace(root, std::max<size_t>(rootTotal * alpha, 1)).first;
        }

        auto targetTotal = rootIt->second;
        auto stageTotal = TasksPerStage[stageId];
        auto stageAlpha = targetTotal / double(stageTotal);

        result[stageId] = ScaleTasks(stageId, stageAlpha);
    }

    return result;
}

TMaxTasksGraph::TTasksPerNode TMaxTasksGraph::ScaleTasks(TStageIdx stageId, double alpha) const {
    const auto& origin = Tasks[stageId];
    const size_t nodeCount = origin.size();
    TMaxTasksGraph::TTasksPerNode result(nodeCount, 0);

    size_t oldTotal = TasksPerStage[stageId];
    if (oldTotal == 0) {
        return result;
    }

    size_t newTotal = std::max<size_t>(oldTotal * alpha, 1);

    std::vector<double> fractions(nodeCount);
    for (size_t j = 0; j < nodeCount; ++j) {
        double scaled = origin[j] * alpha;
        result[j] = scaled;
        fractions[j] = scaled - result[j];
    }

    size_t currentTotal = std::accumulate(result.begin(), result.end(), size_t{0});
    size_t remainder = (newTotal > currentTotal) ? (newTotal - currentTotal) : 0;

    if (remainder > 0) {
        std::vector<size_t> indices(nodeCount);
        std::iota(indices.begin(), indices.end(), 0);
        std::partial_sort(
            indices.begin(),
            indices.begin() + std::min(remainder, nodeCount),
            indices.end(),
            [&](size_t a, size_t b) { return fractions[a] > fractions[b]; }
        );

        for (size_t k = 0; k < std::min(remainder, nodeCount); ++k) {
            result[indices[k]] += 1;
        }
    }

    if (currentTotal + remainder == 0) {
        size_t bestNode = std::distance(origin.begin(), std::max_element(origin.begin(), origin.end()));
        result[bestNode] = 1;
    }

    return result;
}

ui64 TMaxTasksGraph::CountChannelsOnNode(const std::vector<TTasksPerNode>& tasks, TNodeId nodeId) const {
    ui64 totalChannels = 0;

    for (TStageIdx stageId = 0; stageId < Stages.size(); ++stageId) {
        auto tasksOnNode = tasks[stageId][nodeId];
        if (tasksOnNode == 0) {
            continue;
        }

        ui64 channelsPerTask = 0;

        for (TStageIdx input : Inputs[stageId]) {
            if (Stages[stageId].Source && *Stages[stageId].Source == input) {
                channelsPerTask += 1;
            } else {
                channelsPerTask += std::accumulate(tasks[input].begin(), tasks[input].end(), size_t{0});
            }
        }
        for (TStageIdx output : Outputs[stageId]) {
            if (Stages[output].Source && *Stages[output].Source == stageId) {
                channelsPerTask += 1;
            } else {
                channelsPerTask += std::accumulate(tasks[output].begin(), tasks[output].end(), size_t{0});
            }
        }

        totalChannels += tasksOnNode * channelsPerTask;
    }

    return totalChannels;
}

} // namespace NKikimr::NKqp
