#include "max_tasks_graph.h"

#include <util/generic/yexception.h>

#include <numeric>
#include <unordered_map>

namespace NKikimr::NKqp {

TMaxTasksGraph::TMaxTasksGraph(size_t maxChannelsCount) : MaxChannelsCount(maxChannelsCount) {}

void TMaxTasksGraph::AddNodes(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot) {
    YQL_ENSURE(!resourcesSnapshot.empty());

    Y_ENSURE(Nodes.empty() && NodeIds.empty(), "AddNodes must be called on an empty graph");
    Y_ENSURE(Stages.empty(), "AddNodes must be called before any stages are added");

    Nodes.reserve(resourcesSnapshot.size());
    Nodes.resize(resourcesSnapshot.size(), {.MaxChannelsCount=MaxChannelsCount});

    size_t nodeIdx = 0;
    for (const auto& node : resourcesSnapshot) {
        Y_ENSURE(NodeIds.emplace(node.GetNodeId(), nodeIdx++).second);
    }

    CheckInvariants();
}

void TMaxTasksGraph::AddNode(TNodeId node) {
    Y_ENSURE(Stages.empty(), "AddNode must be called before any stages are added");

    auto nodeIt = NodeIds.find(node);
    if (nodeIt == NodeIds.end()) {
        {
            TStringStream ss;
            ss << "Adding node that is not found in resource snapshot: " << node << Endl;
            ss << "Known nodes:";
            for (const auto [knownNode, _] : NodeIds) {
                ss << " " << knownNode;
            }
            ss << Endl;
        }

        NodeIds.emplace(node, Nodes.size());
        Nodes.push_back({.MaxChannelsCount=MaxChannelsCount});
    }

    CheckInvariants();
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
    Stages.push_back(newStage);
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
    Outputs.emplace_back();

    Tasks.emplace_back(Nodes.size(), 0);
    TasksPerStage.push_back(0);

    Y_DEBUG_ABORT_UNLESS(Stages.size() == Inputs.size() && Inputs.size() == Tasks.size() && Inputs.size() == Outputs.size());

    CheckInvariants();
}

void TMaxTasksGraph::AddTasks(const TStageId& stage, TNodeId node, size_t tasksCount) {
    YQL_ENSURE(tasksCount);

    auto stageIdx = StageIds.at(stage);
    auto nodeIdx = NodeIds.find(node);

    Y_ENSURE(nodeIdx != NodeIds.end(), "Trying to add tasks to unknown node: " << node); // TODO: how can there be unknown nodes?

    Tasks.at(stageIdx).at(nodeIdx->second) += tasksCount;
    Nodes.at(nodeIdx->second).TasksCount += tasksCount;
    TasksPerStage.at(stageIdx) += tasksCount;

    CheckInvariants();
}

void TMaxTasksGraph::AddTasks(const TStageId& stage, size_t tasksCount) {
    YQL_ENSURE(tasksCount);

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

    CheckInvariants();
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

void TMaxTasksGraph::Print() const {
    auto& out = Cerr;

    out << "=== TMaxTasksGraph ===" << Endl;
    out << "MaxChannelsCount: " << MaxChannelsCount << Endl;
    out << Endl;

    // Nodes
    out << "--- Nodes (" << Nodes.size() << ") ---" << Endl;
    for (const auto& [nodeId, nodeIdx] : NodeIds) {
        const auto& node = Nodes.at(nodeIdx);
        out << "  Node[" << nodeIdx << "] (id=" << nodeId << ")"
            << " MaxChannels=" << node.MaxChannelsCount
            << " TasksCount=" << node.TasksCount
            << Endl;
    }
    out << Endl;

    // Stages
    out << "--- Stages (" << Stages.size() << ") ---" << Endl;
    for (const auto& [stageId, stageIdx] : StageIds) {
        const auto& stage = Stages[stageIdx];
        const char* typeName = "UNKNOWN";
        switch (stage.Type) {
            case EStageType::FIXED: typeName = "FIXED"; break;
            case EStageType::COPY:  typeName = "COPY";  break;
            case EStageType::ANY:   typeName = "ANY";   break;
        }

        out << "  Stage[" << stageIdx << "] (txId=" << stageId.TxId << ", stageId=" << stageId.StageId << ")"
            << " Type=" << typeName
            << " Source=" << (stage.Source.has_value() ? ToString(*stage.Source) : "none")
            << " RoundRobin=" << stage.RoundRobin
            << " TotalTasks=" << TasksPerStage[stageIdx]
            << Endl;

        // Inputs
        out << "    Inputs: [";
        bool first = true;
        for (auto inputIdx : Inputs[stageIdx]) {
            if (!first) {
                out << ", ";
            }
            out << inputIdx;
            first = false;
        }
        out << "]" << Endl;

        // Outputs
        out << "    Outputs: [";
        first = true;
        for (auto outputIdx : Outputs[stageIdx]) {
            if (!first) {
                out << ", ";
            }
            out << outputIdx;
            first = false;
        }
        out << "]" << Endl;

        // Tasks per node
        out << "    Tasks per node:";
        if (stageIdx < Tasks.size() && !Tasks[stageIdx].empty()) {
            for (size_t nodeIdx = 0; nodeIdx < Tasks[stageIdx].size(); ++nodeIdx) {
                if (Tasks[stageIdx][nodeIdx] > 0) {
                    out << " [node " << nodeIdx << "]=" << Tasks[stageIdx][nodeIdx];
                }
            }
        } else {
            out << " (empty)";
        }
        out << Endl;
    }
    out << Endl;

    // LastFeasible distribution (if available)
    if (!LastFeasible.empty()) {
        out << "--- Last Feasible Distribution ---" << Endl;
        for (size_t stageIdx = 0; stageIdx < LastFeasible.size(); ++stageIdx) {
            out << "  Stage[" << stageIdx << "]:";
            for (size_t nodeIdx = 0; nodeIdx < LastFeasible[stageIdx].size(); ++nodeIdx) {
                if (LastFeasible[stageIdx][nodeIdx] > 0) {
                    out << " [node " << nodeIdx << "]=" << LastFeasible[stageIdx][nodeIdx];
                }
            }
            out << Endl;
        }
        out << Endl;
    }

    out << "=== End TMaxTasksGraph ===" << Endl;
}

bool TMaxTasksGraph::IsFeasible(double alpha) const {
    auto tasks = ComputeScaledTasks(alpha);

    for (TNodeId nodeId = 0; nodeId < Nodes.size(); ++nodeId) {
        uint64_t channels = CountChannelsOnNode(tasks, nodeId);
        if (channels > Nodes.at(nodeId).MaxChannelsCount) {
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

void TMaxTasksGraph::CheckInvariants() const {
    // --- 1) Согласованность размеров основных контейнеров ---
    Y_ENSURE(Nodes.size()  == NodeIds.size(),
        "Nodes/NodeIds size mismatch: " << Nodes.size() << " vs " << NodeIds.size());
    Y_ENSURE(Stages.size() == StageIds.size(),
        "Stages/StageIds size mismatch: " << Stages.size() << " vs " << StageIds.size());
    Y_ENSURE(Stages.size() == Inputs.size(),
        "Stages/Inputs size mismatch: " << Stages.size() << " vs " << Inputs.size());
    Y_ENSURE(Stages.size() == Outputs.size(),
        "Stages/Outputs size mismatch: " << Stages.size() << " vs " << Outputs.size());
    Y_ENSURE(Stages.size() == Tasks.size(),
        "Stages/Tasks size mismatch: " << Stages.size() << " vs " << Tasks.size());
    Y_ENSURE(Stages.size() == TasksPerStage.size(),
        "Stages/TasksPerStage size mismatch: " << Stages.size() << " vs " << TasksPerStage.size());

    // --- 2) NodeIds: значения - валидные индексы в Nodes, без дыр ---
    {
        std::vector<bool> seenNode(Nodes.size(), false);
        for (const auto& [nodeId, idx] : NodeIds) {
            Y_ENSURE(idx < Nodes.size(),
                "NodeIds entry (nodeId=" << nodeId << ") has out-of-range idx=" << idx);
            Y_ENSURE(!seenNode[idx],
                "NodeIds has duplicate idx=" << idx << " (nodeId=" << nodeId << ")");
            seenNode[idx] = true;
        }
        for (size_t i = 0; i < Nodes.size(); ++i) {
            Y_ENSURE(seenNode[i], "Node idx=" << i << " is not present in NodeIds");
        }
    }

    // --- 3) StageIds: значения - валидные индексы в Stages, без дыр ---
    {
        std::vector<bool> seenStage(Stages.size(), false);
        for (const auto& [stageKey, idx] : StageIds) {
            Y_ENSURE(idx < Stages.size(),
                "StageIds entry (txId=" << stageKey.TxId << ", stageId=" << stageKey.StageId
                << ") has out-of-range idx=" << idx);
            Y_ENSURE(!seenStage[idx],
                "StageIds has duplicate idx=" << idx);
            seenStage[idx] = true;
        }
        for (size_t i = 0; i < Stages.size(); ++i) {
            Y_ENSURE(seenStage[i], "Stage idx=" << i << " is not present in StageIds");
        }
    }

    // --- 4) Ширина каждой строки Tasks равна числу нод ---
    for (size_t s = 0; s < Tasks.size(); ++s) {
        Y_ENSURE(Tasks[s].size() == Nodes.size(),
            "Tasks[" << s << "].size()=" << Tasks[s].size()
            << " != Nodes.size()=" << Nodes.size());
    }

    // --- 5) Stage.Source и Stage.RoundRobin ---
    for (size_t s = 0; s < Stages.size(); ++s) {
        const auto& stage = Stages[s];
        if (stage.Source) {
            Y_ENSURE(*stage.Source < s,
                "Stage[" << s << "].Source=" << *stage.Source << " must be < " << s);
            // После нормализации в AddStage Source никогда не должен указывать на COPY.
            Y_ENSURE(Stages[*stage.Source].Type != COPY,
                "Stage[" << s << "].Source=" << *stage.Source << " points to a COPY stage");
            // У COPY должен быть Source; у не-COPY (после AddStage) — он опционален.
        }
        if (stage.Type == COPY) {
            Y_ENSURE(stage.Source.has_value(),
                "COPY Stage[" << s << "] has no Source");
        }
        Y_ENSURE(!Nodes.empty() ? stage.RoundRobin < Nodes.size() : stage.RoundRobin == 0,
            "Stage[" << s << "].RoundRobin=" << stage.RoundRobin
            << " out of range (Nodes.size()=" << Nodes.size() << ")");
    }

    // --- 6) Inputs/Outputs: индексы валидны, DAG topologически упорядочен, симметрия ---
    for (size_t s = 0; s < Stages.size(); ++s) {
        for (auto in : Inputs[s]) {
            Y_ENSURE(in < Stages.size(),
                "Inputs[" << s << "] has out-of-range idx=" << in);
            Y_ENSURE(in < s,
                "Inputs[" << s << "]=" << in << " must be < " << s << " (topological order)");
            // Должна быть встречная запись в Outputs[in]
            const auto& outs = Outputs[in];
            Y_ENSURE(std::ranges::find(outs, s) != outs.end(),
                "Inputs[" << s << "] contains " << in
                << " but Outputs[" << in << "] does not contain " << s);
        }
        for (auto out : Outputs[s]) {
            Y_ENSURE(out < Stages.size(),
                "Outputs[" << s << "] has out-of-range idx=" << out);
            Y_ENSURE(out > s,
                "Outputs[" << s << "]=" << out << " must be > " << s << " (topological order)");
            const auto& ins = Inputs[out];
            Y_ENSURE(std::ranges::find(ins, s) != ins.end(),
                "Outputs[" << s << "] contains " << out
                << " but Inputs[" << out << "] does not contain " << s);
        }
    }

    // --- 7) TasksPerStage == sum(Tasks[s]); агрегаты по нодам совпадают с Nodes[n].TasksCount ---
    {
        std::vector<size_t> perNode(Nodes.size(), 0);
        for (size_t s = 0; s < Stages.size(); ++s) {
            size_t rowSum = 0;
            for (size_t n = 0; n < Nodes.size(); ++n) {
                rowSum += Tasks[s][n];
                perNode[n] += Tasks[s][n];
            }
            Y_ENSURE(rowSum == TasksPerStage[s],
                "TasksPerStage[" << s << "]=" << TasksPerStage[s]
                << " != sum(Tasks[" << s << "])=" << rowSum);
        }
        for (size_t n = 0; n < Nodes.size(); ++n) {
            Y_ENSURE(perNode[n] == Nodes.at(n).TasksCount,
                "Nodes[" << n << "].TasksCount=" << Nodes.at(n).TasksCount
                << " != sum over stages=" << perNode[n]);
        }
    }
}

} // namespace NKikimr::NKqp
