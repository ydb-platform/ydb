#include "max_tasks_graph.h"

#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <algorithm>
#include <numeric>

namespace NKikimr::NKqp {

namespace {
    size_t Total(const std::vector<size_t>& perNode) {
        return std::accumulate(perNode.begin(), perNode.end(), size_t{0});
    }
}

TMaxTasksGraph::TMaxTasksGraph(size_t maxChannelsCount, TTaskResourceEstimationParams estimationParams)
    : MaxChannelsCount(maxChannelsCount)
    , EstimationParams(estimationParams)
{}

void TMaxTasksGraph::AddNodes(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot) {
    YQL_ENSURE(!resourcesSnapshot.empty());

    Y_ENSURE(NodeIds.empty(), "AddNodes must be called on an empty graph");
    Y_ENSURE(Stages.empty(), "AddNodes must be called before any stages are added");

    for (const auto& node : resourcesSnapshot) {
        Y_ENSURE(NodeIds.emplace(node.GetNodeId(), NodeIdByIdx.size()).second);
        NodeIdByIdx.push_back(node.GetNodeId());
        NodeResources.push_back(TNodeResources{
            .RemainsMemory = node.GetTotalMemory() - node.GetUsedMemory(),
            .RemainsTasks = node.GetAvailableComputeActors(), // one task == one compute actor.
            .DataCenterId = node.GetKqpProxyNodeResources().GetDataCenterId(),
        });
    }

    CheckInvariants();
}

void TMaxTasksGraph::AddNode(TNodeId node) {
    Y_ENSURE(Stages.empty(), "AddNode must be called before any stages are added");

    if (!NodeIds.contains(node)) {
        NodeIds.emplace(node, NodeIdByIdx.size());
        NodeIdByIdx.push_back(node);
        NodeResources.emplace_back(); // no snapshot entry: zero budgets, fine for round-robin.
    }

    CheckInvariants();
}

void TMaxTasksGraph::AddStage(TStageInfo& stageInfo, EStageType type, const std::list<TStageId>& inputs, std::optional<TStageId> copyInput) {
    TStage newStage;
    newStage.Info = &stageInfo;
    newStage.Type = type;
    newStage.Source = copyInput ? std::optional{StageIds.at(*copyInput)} : std::nullopt;

    Y_DEBUG_ABORT_UNLESS((newStage.Type == COPY) == newStage.Source.has_value());

    if (type == COPY) {
        const auto& prevStage = Stages.at(*newStage.Source);
        if (prevStage.Type == FIXED) {
            newStage.Type = FIXED;
        }
        if (prevStage.Source) {
            newStage.Source = prevStage.Source; // normalize to the group root.
        }
    }

    for (const auto& input : inputs) {
        newStage.Inputs.push_back(StageIds.at(input));
    }

    const TStageIdx newStageIdx = Stages.size();
    for (TStageIdx inputIdx : newStage.Inputs) {
        Stages.at(inputIdx).Outputs.push_back(newStageIdx);
    }

    // Group assignment: a follower joins its root's group, a root starts a new one.
    if (newStage.Source) {
        newStage.Group = Stages.at(*newStage.Source).Group;
        Groups.at(newStage.Group).Stages.push_back(newStageIdx);
    } else {
        newStage.Group = Groups.size();
        TGroup group;
        group.Root = newStageIdx;
        group.Fixed = (newStage.Type == FIXED);
        group.Stages.push_back(newStageIdx);
        Groups.push_back(std::move(group));
    }

    Y_ENSURE(StageIds.emplace(stageInfo.Id, newStageIdx).second);
    Stages.push_back(std::move(newStage));

    CheckInvariants();
}

TMaxTasksGraph::TNodeIdx TMaxTasksGraph::ResolveNodeIdx(TNodeId node) const {
    auto nodeIt = NodeIds.find(node);
    Y_ENSURE(nodeIt != NodeIds.end(), "Trying to add task to unknown node: " << node); // TODO: how can there be unknown nodes?
    return nodeIt->second;
}

void TMaxTasksGraph::AddTask(const TTask& task, std::optional<TNodeId> node) {
    const TStageIdx stageIdx = StageIds.at(task.StageId);
    auto& stage = Stages.at(stageIdx);
    auto& group = Groups.at(stage.Group);

    const size_t columnIdx = stage.Tasks.size();
    stage.Tasks.push_back(task.Id);

    if (stageIdx == group.Root) {
        // The root defines the group's columns.
        Y_ENSURE(group.ColumnNodes.size() == columnIdx);
        if (node) {
            group.ColumnNodes.push_back(ResolveNodeIdx(*node)); // pinned column.
        } else {
            group.ColumnNodes.push_back(std::nullopt);          // free column, placed in DistributeTasksToNodes.
        }
    } else {
        // A follower task joins an existing column and inherits its node. Normally the `node` argument is nullopt for
        // copy tasks; the one exception is a stage that must run on a specific node regardless of the copy connection
        // (a buffer-actor write - see StageNeedsLocalPlacement). In that case pin the whole shared column, and require
        // it not to clash with an existing pin (e.g. the root being a scan already tied to a shard node).
        Y_ENSURE(columnIdx < group.ColumnNodes.size(), "follower stage has more tasks than its group root");
        if (node) {
            const TNodeIdx nodeIdx = ResolveNodeIdx(*node);
            auto& columnNode = group.ColumnNodes[columnIdx];
            Y_ENSURE(!columnNode || *columnNode == nodeIdx,
                "Cannot pin copy-group column " << columnIdx << " to node " << *node << ": already pinned to node "
                    << NodeIdByIdx[*columnNode]);
            columnNode = nodeIdx;
        }
    }

    CheckInvariants();
}

void TMaxTasksGraph::PlaceColumnOnNode(TGroup& group, size_t columnIdx, TNodeIdx node) {
    group.ColumnNodes.at(columnIdx) = node;
}

void TMaxTasksGraph::EstimateTasksResources() {
    // Mirror TKqpPlanner: it estimated every task's memory with a single coarse task count - the total number of tasks
    // being placed - not the per-stage count. Dividing the mkql limit by the per-stage count would give a low-parallelism
    // stage (e.g. a 1-task result stage) the full mkql limit, turning its column into a memory outlier that sorts first
    // and pre-loads a node, skewing larger sibling stages. Keep the coarse count so the placement matches the planner.
    size_t totalTasks = 0;
    for (const auto& stage : Stages) {
        totalTasks += stage.Tasks.size();
    }

    for (auto& group : Groups) {
        TColumnCost cost;
        for (TStageIdx stageIdx : group.Stages) {
            const auto& stage = Stages[stageIdx];
            if (stage.Tasks.empty()) {
                continue; // empty stage (e.g. a COPY of an empty input) contributes no task to the column.
            }

            // Mirror TKqpPlanner::BuildInitialTaskResources: a single channel buffer per direction that exists, and the
            // mkql class taken from the stage program.
            TTaskResourceEstimation est;
            est.ChannelBuffersCount = (stage.Info->InputsCount ? 1 : 0) + (stage.Info->OutputsCount ? 1 : 0);
            est.HeavyProgram = stage.Info->Meta.GetStage(stage.Info->Id).GetProgram().GetSettings().GetHasMapJoin();
            EstimateTaskResources(est, EstimationParams, totalTasks);

            cost.Memory += est.TotalMemoryLimit;
            cost.Tasks += 1; // one task of this stage per column.
        }
        group.ColumnCost = cost;
    }
}

void TMaxTasksGraph::DistributeTasksToNodes() {
    if (NodesCount() == 0) {
        return;
    }

    // Resource-aware placement first; round-robin only when it can't fit everything (e.g. nodes report no budget, as in
    // unit tests, or the cluster is genuinely too small). Pinned columns are kept by both paths (idempotent).
    if (!DistributeByResources()) {
        DistributeRoundRobin();
    }

    CheckInvariants();
}

bool TMaxTasksGraph::DistributeByResources() {
    // Task memory can grow during execution; reserve a bit more than the estimate (mirrors KqpPlanner).
    constexpr double memoryOverflow = 1.2;

    const auto memoryCost = [&](const TGroup& group) {
        return static_cast<ui64>(group.ColumnCost.Memory * memoryOverflow);
    };

    // Working per-node budget, started from the snapshot and pre-charged with the already-pinned columns.
    std::vector<ui64> freeMemory(NodesCount());
    std::vector<ui32> freeTasks(NodesCount());
    std::vector<size_t> tasksOnNode(NodesCount(), 0); // load metric for the spread tie-break.
    for (TNodeIdx n = 0; n < NodesCount(); ++n) {
        freeMemory[n] = NodeResources[n].RemainsMemory;
        freeTasks[n] = NodeResources[n].RemainsTasks;
    }

    std::vector<std::pair<TGroupIdx, size_t>> freeColumns; // (group, column index) of every column still to place.
    for (TGroupIdx g = 0; g < Groups.size(); ++g) {
        const auto& group = Groups[g];
        for (size_t columnIdx = 0; columnIdx < group.ColumnNodes.size(); ++columnIdx) {
            if (const auto& node = group.ColumnNodes[columnIdx]) {
                const TNodeIdx n = *node;
                freeMemory[n] -= std::min(freeMemory[n], memoryCost(group));
                freeTasks[n] -= std::min<ui32>(freeTasks[n], group.ColumnCost.Tasks);
                tasksOnNode[n] += group.ColumnCost.Tasks;
            } else {
                freeColumns.emplace_back(g, columnIdx);
            }
        }
    }

    // Place the heaviest columns first: harder-to-fit columns get the pick of the nodes (mirrors KqpPlanner). Stable, so
    // equal-cost columns keep creation (group) order and stay contiguous per group - otherwise a small group's columns
    // could interleave into a large sibling and perturb its otherwise-even spread.
    std::ranges::stable_sort(freeColumns, [&](const auto& lhs, const auto& rhs) {
        return Groups[lhs.first].ColumnCost.Memory > Groups[rhs.first].ColumnCost.Memory;
    });

    std::vector<TNodeIdx> placement(freeColumns.size()); // chosen node per free column; applied only if all fit.
    for (size_t i = 0; i < freeColumns.size(); ++i) {
        const auto& group = Groups[freeColumns[i].first];
        const ui64 memNeed = memoryCost(group);
        const ui32 taskNeed = group.ColumnCost.Tasks;

        // A real column always holds at least one task; zero means EstimateTasksResources hasn't run (e.g. unit tests),
        // so there is no resource signal to place by - fall back to round-robin.
        if (taskNeed == 0) {
            return false;
        }

        // Least-loaded fitting node, tie-broken by most free memory, then most free task slots.
        std::optional<TNodeIdx> best;
        for (TNodeIdx n = 0; n < NodesCount(); ++n) {
            if (freeMemory[n] < memNeed || freeTasks[n] < taskNeed) {
                continue;
            }
            if (!best) {
                best = n;
            } else if (tasksOnNode[n] != tasksOnNode[*best]) {
                if (tasksOnNode[n] < tasksOnNode[*best]) { best = n; }
            } else if (freeMemory[n] != freeMemory[*best]) {
                if (freeMemory[n] > freeMemory[*best]) { best = n; }
            } else if (freeTasks[n] > freeTasks[*best]) {
                best = n;
            }
        }

        if (!best) {
            return false; // doesn't fit anywhere - bail out without touching the placement.
        }

        placement[i] = *best;
        freeMemory[*best] -= memNeed;
        freeTasks[*best] -= taskNeed;
        tasksOnNode[*best] += taskNeed;
    }

    for (size_t i = 0; i < freeColumns.size(); ++i) {
        PlaceColumnOnNode(Groups[freeColumns[i].first], freeColumns[i].second, placement[i]);
    }
    return true;
}

void TMaxTasksGraph::DistributeRoundRobin() {
    // Place every free column round-robin. Pinned columns already have a node and are left untouched (idempotent).
    for (auto& group : Groups) {
        TNodeIdx roundRobin = 0;
        for (size_t columnIdx = 0; columnIdx < group.ColumnNodes.size(); ++columnIdx) {
            if (!group.ColumnNodes[columnIdx]) {
                PlaceColumnOnNode(group, columnIdx, roundRobin);
                if (++roundRobin == NodesCount()) {
                    roundRobin = 0;
                }
            }
        }
    }
}

std::vector<TMaxTasksGraph::TColumnsPerNode> TMaxTasksGraph::GroupColumns() const {
    std::vector<TColumnsPerNode> result(Groups.size(), TColumnsPerNode(NodesCount(), 0));
    for (TGroupIdx g = 0; g < Groups.size(); ++g) {
        for (const auto& columnNode : Groups[g].ColumnNodes) {
            Y_ENSURE(columnNode.has_value(), "Shrink before all columns are placed (call DistributeTasksToNodes first)");
            result[g][*columnNode]++;
        }
    }
    return result;
}

void TMaxTasksGraph::Shrink() {
    if (Stages.empty() || NodesCount() == 0) {
        return;
    }

    // TODO: verify that there are no empty stages.

    const auto base = GroupColumns();

    std::vector<size_t> columnsPerNode(NodesCount(), 0);
    for (const auto& groupColumns : base) {
        for (TNodeIdx n = 0; n < NodesCount(); ++n) {
            columnsPerNode[n] += groupColumns[n];
        }
    }

    double lo = 0.0;
    double hi = 1.0;

    if (IsFeasible(base, hi) || !IsFeasible(base, lo)) {
        return;
    }

    // Binary search the global scale coefficient.
    const int maxIterations = 50;
    const double epsilon = 1 / double(*std::ranges::max_element(columnsPerNode));

    for (int i = 0; i < maxIterations && (hi - lo > epsilon); ++i) {
        double mid = (lo + hi) / 2.0;
        if (IsFeasible(base, mid)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    // Apply the last feasible distribution: per group, keep the first LastFeasible[g][n] columns on each node (in
    // creation order) and drop the rest - removing a column drops its task from every member stage at once.
    for (TGroupIdx g = 0; g < Groups.size(); ++g) {
        auto& group = Groups[g];
        const auto& target = LastFeasible[g];

        std::vector<size_t> kept(NodesCount(), 0);
        std::vector<bool> survives(group.ColumnNodes.size(), false);
        for (size_t columnIdx = 0; columnIdx < group.ColumnNodes.size(); ++columnIdx) {
            const TNodeIdx n = *group.ColumnNodes[columnIdx];
            if (kept[n] < target[n]) {
                survives[columnIdx] = true;
                kept[n]++;
            }
        }

        std::vector<std::optional<TNodeIdx>> survivingColumns;
        for (size_t columnIdx = 0; columnIdx < survives.size(); ++columnIdx) {
            if (survives[columnIdx]) {
                survivingColumns.push_back(group.ColumnNodes[columnIdx]);
            }
        }
        group.ColumnNodes = std::move(survivingColumns);

        for (TStageIdx stageIdx : group.Stages) {
            auto& tasks = Stages[stageIdx].Tasks;
            std::vector<ui64> survivingTasks;
            for (size_t columnIdx = 0; columnIdx < survives.size(); ++columnIdx) {
                if (survives[columnIdx]) {
                    survivingTasks.push_back(tasks[columnIdx]);
                }
            }
            tasks = std::move(survivingTasks);
        }
    }
}

void TMaxTasksGraph::PlaceTasks(TKqpTasksGraph& graph) {
    auto& tasks = graph.GetTasks();

    // Mark survivors: every task Id still referenced after Shrink.
    std::vector<bool> survives(tasks.size() + 1, false);
    for (const auto& stage : Stages) {
        for (ui64 id : stage.Tasks) {
            survives.at(id) = true;
        }
    }

    // Rebuild the graph's task list keeping survivors, renumbering them so Ids stay contiguous.
    TVector<TTask> compacted;
    std::vector<ui64> idMap(tasks.size() + 1, 0); // old Id -> new Id (0 = removed)
    for (auto& task : tasks) {
        const ui64 oldId = task.Id;
        if (survives[oldId]) {
            auto& kept = compacted.emplace_back(std::move(task));
            kept.Id = compacted.size();
            idMap[oldId] = kept.Id;
        }
    }
    tasks = std::move(compacted);

    // Lay the surviving tasks into stageInfo.Tasks (node-major order), remapping Ids and stamping ExpectedNodeId. Every
    // member stage of a group emits columns in the same order, so co-located tasks stay index-aligned across the group.
    for (auto& stage : Stages) {
        const auto& group = Groups[stage.Group];

        std::vector<std::vector<ui64>> byNode(NodesCount());
        for (size_t columnIdx = 0; columnIdx < stage.Tasks.size(); ++columnIdx) {
            const TNodeIdx n = *group.ColumnNodes[columnIdx];
            byNode[n].push_back(idMap.at(stage.Tasks[columnIdx]));
        }

        auto& stageTasks = stage.Info->Tasks;
        stageTasks.clear();
        for (TNodeIdx n = 0; n < NodesCount(); ++n) {
            for (ui64 id : byNode[n]) {
                graph.GetTask(id).Meta.ExpectedNodeId = NodeIdByIdx[n];
                stageTasks.push_back(id);
            }
        }
    }

    // Check that tasks from one column are placed on the same node.
    for (const auto& group : Groups) {
        const auto& rootTasks = Stages[group.Root].Info->Tasks;
        for (TStageIdx memberIdx : group.Stages) {
            if (memberIdx == group.Root) {
                continue;
            }

            const auto& memberTasks = Stages[memberIdx].Info->Tasks;
            Y_ENSURE(memberTasks.size() == rootTasks.size(),
                "Copy-group placement mismatch: root stage[" << group.Root << "] has " << rootTasks.size()
                    << " tasks, but member stage[" << memberIdx << "] has " << memberTasks.size());

            for (size_t i = 0; i < memberTasks.size(); ++i) {
                const auto& rootNode = graph.GetTask(rootTasks[i]).Meta.ExpectedNodeId;
                const auto& memberNode = graph.GetTask(memberTasks[i]).Meta.ExpectedNodeId;
                Y_ENSURE(rootNode && memberNode && *rootNode == *memberNode,
                    "Copy-group placement mismatch: root stage[" << group.Root << "] task[" << i << "] is on node "
                        << (rootNode ? ToString(*rootNode) : TString("<none>")) << ", but member stage[" << memberIdx
                        << "] task[" << i << "] (same column) is on node "
                        << (memberNode ? ToString(*memberNode) : TString("<none>")));
            }
        }
    }
}

size_t TMaxTasksGraph::GetStageTasksCount(const TStageId& stage, TNodeId node) const {
    const auto& group = Groups.at(Stages.at(StageIds.at(stage)).Group);
    const TNodeIdx nodeIdx = NodeIds.at(node);
    size_t count = 0;
    for (const auto& columnNode : group.ColumnNodes) {
        if (columnNode == nodeIdx) {
            count++;
        }
    }
    return count;
}

size_t TMaxTasksGraph::GetStageTasksCount(const TStageId& stage) const {
    return Stages.at(StageIds.at(stage)).Tasks.size();
}

bool TMaxTasksGraph::IsFeasible(const std::vector<TColumnsPerNode>& base, double alpha) const {
    auto columns = ComputeScaledColumns(base, alpha);

    for (TNodeIdx nodeIdx = 0; nodeIdx < NodesCount(); ++nodeIdx) {
        if (CountChannelsOnNode(columns, nodeIdx) > MaxChannelsCount) {
            return false;
        }
    }

    LastFeasible = std::move(columns);

    return true;
}

std::vector<TMaxTasksGraph::TColumnsPerNode> TMaxTasksGraph::ComputeScaledColumns(const std::vector<TColumnsPerNode>& base, double alpha) const {
    if (alpha == 1.0) {
        return base;
    }

    // Groups are independent placement units: a FIXED group keeps its columns, any other group is scaled by alpha.
    // (Copy stages don't need special handling here - they share their group's columns by construction.)
    std::vector<TColumnsPerNode> result(Groups.size());
    for (TGroupIdx g = 0; g < Groups.size(); ++g) {
        result[g] = Groups[g].Fixed ? base[g] : ScaleColumns(base[g], alpha);
    }
    return result;
}

TMaxTasksGraph::TColumnsPerNode TMaxTasksGraph::ScaleColumns(const TColumnsPerNode& origin, double alpha) const {
    const size_t nodeCount = origin.size();
    TColumnsPerNode result(nodeCount, 0);

    const size_t oldTotal = Total(origin);
    if (oldTotal == 0) {
        return result;
    }

    const size_t newTotal = std::max<size_t>(oldTotal * alpha, 1);

    std::vector<double> fractions(nodeCount);
    for (size_t j = 0; j < nodeCount; ++j) {
        double scaled = origin[j] * alpha;
        result[j] = scaled;
        fractions[j] = scaled - result[j];
    }

    size_t currentTotal = Total(result);
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

size_t TMaxTasksGraph::CountChannelsOnNode(const std::vector<TColumnsPerNode>& columns, TNodeIdx nodeIdx) const {
    std::vector<size_t> groupTotal(columns.size());
    for (TGroupIdx g = 0; g < columns.size(); ++g) {
        groupTotal[g] = Total(columns[g]);
    }

    ui64 totalChannels = 0;

    for (TStageIdx stageIdx = 0; stageIdx < Stages.size(); ++stageIdx) {
        const auto& stage = Stages[stageIdx];
        const auto tasksOnNode = columns[stage.Group][nodeIdx];
        if (tasksOnNode == 0) {
            continue;
        }

        ui64 channelsPerTask = 0;

        // An edge within the same group is a copy connection: 1 local channel per task (the paired task is co-located).
        // An edge to another group is a full mesh: a channel to every task of the other stage.
        for (TStageIdx input : stage.Inputs) {
            const TGroupIdx inputGroup = Stages[input].Group;
            channelsPerTask += (inputGroup == stage.Group) ? 1 : groupTotal[inputGroup];
        }
        for (TStageIdx output : stage.Outputs) {
            const TGroupIdx outputGroup = Stages[output].Group;
            channelsPerTask += (outputGroup == stage.Group) ? 1 : groupTotal[outputGroup];
        }

        totalChannels += tasksOnNode * channelsPerTask;
    }

    return totalChannels;
}

TString TMaxTasksGraph::DumpToString() const {
    TStringStream out;

    out << "=== TMaxTasksGraph ===" << Endl;
    out << "MaxChannelsCount: " << MaxChannelsCount << Endl;

    out << "--- Nodes (" << NodesCount() << ") ---" << Endl;
    for (TNodeIdx n = 0; n < NodesCount(); ++n) {
        const auto& res = NodeResources[n];
        out << "  Node[" << n << "] (id=" << NodeIdByIdx[n] << ")"
            << " Memory=" << res.RemainsMemory
            << " Tasks=" << res.RemainsTasks
            << " DC=" << res.DataCenterId
            << Endl;
    }

    out << "--- Stages (" << Stages.size() << ") ---" << Endl;
    for (TStageIdx s = 0; s < Stages.size(); ++s) {
        const auto& stage = Stages[s];
        const char* typeName = stage.Type == FIXED ? "FIXED" : (stage.Type == COPY ? "COPY" : "ANY");

        out << "  Stage[" << s << "] (" << stage.Info->Id << ")"
            << " Type=" << typeName
            << " Group=" << stage.Group
            << " TotalTasks=" << stage.Tasks.size()
            << Endl;
        out << "    Inputs: [" << JoinSeq(", ", stage.Inputs) << "]" << Endl;
        out << "    Outputs: [" << JoinSeq(", ", stage.Outputs) << "]" << Endl;

        // Node histogram: tasks-on-a-node -> number of nodes hosting that many tasks of the stage. Same diagram the
        // tests assert via TTaskDistribution::NodeHistogram, so its output can be transcribed straight into the
        // per-stage expected tables.
        const auto& group = Groups[stage.Group];
        THashMap<TNodeIdx, size_t> perNode;
        for (size_t c = 0; c < stage.Tasks.size() && c < group.ColumnNodes.size(); ++c) {
            if (const auto& node = group.ColumnNodes[c]) {
                perNode[*node]++;
            }
        }
        std::map<size_t, size_t> histogram; // tasksOnNode -> nodeCount, ordered for stable output.
        for (const auto& [node, count] : perNode) {
            histogram[count]++;
        }
        out << "    Node histogram (tasksOnNode -> nodes): {";
        bool first = true;
        for (const auto& [tasksOnNode, nodeCount] : histogram) {
            out << (first ? " " : ", ") << tasksOnNode << " -> " << nodeCount;
            first = false;
        }
        out << " }" << Endl;
    }

    out << "--- Groups (" << Groups.size() << ") ---" << Endl;
    for (TGroupIdx g = 0; g < Groups.size(); ++g) {
        const auto& group = Groups[g];
        out << "  Group[" << g << "] Root=" << group.Root << (group.Fixed ? " FIXED" : "")
            << " Columns=" << group.ColumnNodes.size()
            << " ColumnCost={mem=" << group.ColumnCost.Memory << ", tasks=" << group.ColumnCost.Tasks << "}"
            << Endl;
        out << "    Columns per node:";
        TColumnsPerNode perNode(NodesCount(), 0);
        for (const auto& columnNode : group.ColumnNodes) {
            if (columnNode) {
                perNode[*columnNode]++;
            }
        }
        for (TNodeIdx n = 0; n < NodesCount(); ++n) {
            if (perNode[n] > 0) {
                out << " [node " << n << "]=" << perNode[n];
            }
        }
        out << Endl;
    }

    out << "=== End TMaxTasksGraph ===" << Endl;

    return out.Str();
}

void TMaxTasksGraph::CheckInvariants() const {
    Y_ENSURE(NodeIds.size() == NodeIdByIdx.size(),
        "NodeIds/NodeIdByIdx size mismatch: " << NodeIds.size() << " vs " << NodeIdByIdx.size());
    Y_ENSURE(NodeResources.size() == NodeIdByIdx.size(),
        "NodeResources/NodeIdByIdx size mismatch: " << NodeResources.size() << " vs " << NodeIdByIdx.size());
    Y_ENSURE(StageIds.size() == Stages.size(),
        "StageIds/Stages size mismatch: " << StageIds.size() << " vs " << Stages.size());

    for (TStageIdx s = 0; s < Stages.size(); ++s) {
        const auto& stage = Stages[s];

        Y_ENSURE(stage.Info, "Stage[" << s << "] has no StageInfo");
        Y_ENSURE(StageIds.at(stage.Info->Id) == s, "StageIds/Stages mismatch for stage idx " << s);
        Y_ENSURE(stage.Group < Groups.size(), "Stage[" << s << "].Group=" << stage.Group << " out of range");

        const auto& group = Groups[stage.Group];
        // While a stage is being filled its tasks grow up to the group's column count; the root grows in lockstep.
        Y_ENSURE(stage.Tasks.size() <= group.ColumnNodes.size(),
            "Stage[" << s << "] has more tasks (" << stage.Tasks.size() << ") than its group columns (" << group.ColumnNodes.size() << ")");
        if (s == group.Root) {
            Y_ENSURE(stage.Tasks.size() == group.ColumnNodes.size(),
                "Root stage[" << s << "] tasks (" << stage.Tasks.size() << ") != group columns (" << group.ColumnNodes.size() << ")");
            Y_ENSURE(!stage.Source.has_value(), "Root stage[" << s << "] must not have a Source");
        }

        // Source (group leader): points backwards, is never a COPY (normalized in AddStage), shares the same group.
        if (stage.Source) {
            Y_ENSURE(*stage.Source < s, "Stage[" << s << "].Source=" << *stage.Source << " must be < " << s);
            Y_ENSURE(Stages[*stage.Source].Type != COPY, "Stage[" << s << "].Source points to a COPY stage");
            Y_ENSURE(Stages[*stage.Source].Group == stage.Group, "Stage[" << s << "] is not in its Source's group");
        }
        if (stage.Type == COPY) {
            Y_ENSURE(stage.Source.has_value(), "COPY Stage[" << s << "] has no Source");
        }

        // Inputs/Outputs: topologically ordered and symmetric.
        for (TStageIdx in : stage.Inputs) {
            Y_ENSURE(in < s, "Inputs[" << s << "]=" << in << " must be < " << s << " (topological order)");
            const auto& outs = Stages[in].Outputs;
            Y_ENSURE(std::ranges::find(outs, s) != outs.end(),
                "Inputs[" << s << "] contains " << in << " but Outputs[" << in << "] does not contain " << s);
        }
        for (TStageIdx out : stage.Outputs) {
            Y_ENSURE(out > s, "Outputs[" << s << "]=" << out << " must be > " << s << " (topological order)");
            const auto& ins = Stages[out].Inputs;
            Y_ENSURE(std::ranges::find(ins, s) != ins.end(),
                "Outputs[" << s << "] contains " << out << " but Inputs[" << out << "] does not contain " << s);
        }
    }
}

} // namespace NKikimr::NKqp
