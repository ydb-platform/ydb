#pragma once

#include "kqp_tasks_graph.h"

#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/protos/kqp.pb.h>

#include <list>
#include <optional>
#include <vector>

namespace NKikimr::NKqp {

// Reflects the original TKqpTasksGraph using tasks count instead of tasks and links stages with tasks dependencies.
//
// Placement is column-based. A "column" is one replica slot of a copy group: it holds exactly one task from every stage
// of the group and lives entirely on a single node. A standalone stage is a group of one stage, so its columns are just
// its tasks. Placing a column onto a node (PlaceColumnOnNode) is the basic action of the placement algorithm, and Shrink
// drops whole columns - this keeps copy-paired tasks co-located on the same node and their per-stage counts equal.
class TMaxTasksGraph {
    using TNodeIdx = size_t;
    using TStageIdx = size_t;
    using TGroupIdx = size_t;

    using TNodeId = ui64;
    using TStageId = NYql::NDq::TStageId;

    using TColumnsPerNode = std::vector<size_t>; // TNodeIdx -> number of columns on the node

public:
    enum EStageType : ui8 {
        FIXED, // fixed number of tasks
        COPY,  // copies number of tasks from one of previous stages
        ANY,   // any number of tasks
    };

    explicit TMaxTasksGraph(size_t maxChannelsCount, TTaskResourceEstimationParams estimationParams = {});

    void AddNodes(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot);
    void AddNode(TNodeId node); // TODO: it's workaround. remove later.

    // Registers a stage. The graph keeps a reference to stageInfo: it reads stageInfo.Id and later (in PlaceTasks)
    // writes the placed task Ids back into stageInfo.Tasks. A COPY stage joins the group of its copyInput.
    void AddStage(TStageInfo& stageInfo, EStageType type, const std::list<TStageId>& inputs, std::optional<TStageId> copyInput = std::nullopt);

    // Registers a task. Only the task Id is stored; the task itself stays in TKqpTasksGraph. For a copy group's root the
    // task starts a new column (pinned to `node`, or left unplaced for DistributeTasksToNodes if `node` is empty); for a
    // follower stage the task joins the matching column and inherits its node (the `node` argument is ignored).
    void AddTask(const TTask& task, std::optional<TNodeId> node);

    // Fills each group's per-column resource cost (memory / compute actors), consumed by the resource-aware placement in
    // DistributeTasksToNodes. Works off the per-stage info (kept via AddStage) plus the estimation params, so it needs
    // neither the task objects nor the node snapshot (node budgets are already captured in AddNodes). The cost is uniform
    // across a group's columns - it depends only on stage-level signals (inputs/outputs presence, HasMapJoin) and the
    // per-stage task count - so it is stored once per group (see TGroup::ColumnCost).
    void EstimateTasksResources();

    // Places every still-unplaced (free) column onto a node. Currently round-robin per group; this is the seam where the
    // greedy resource-aware strategy from KqpPlanner will plug in. Idempotent: already-placed (pinned) columns are kept.
    // TODO: replace round-robin with the resource-aware placement, using EstimateTasksResources output.
    void DistributeTasksToNodes();

    void Shrink(); // TODO: forbid double call - it's not idempotent.

    // Materializes the placement onto the graph: drops the tasks of the columns that didn't survive Shrink, renumbers
    // the survivors so their Ids stay contiguous, stamps every survivor's Meta.ExpectedNodeId and fills stageInfo.Tasks
    // (node-major order). Runs once, before channels are built.
    void PlaceTasks(TKqpTasksGraph& graph);

    size_t GetStageTasksCount(const TStageId& stage, TNodeId node) const;
    size_t GetStageTasksCount(const TStageId& stage) const;

    TString DumpToString() const;

private:
    // Per-node budget read from the resources snapshot. Used by the greedy resource-aware placement (the seam in
    // DistributeTasksToNodes). Nodes added via the AddNode workaround have no snapshot entry and keep zero budgets.
    struct TNodeResources {
        ui64 RemainsMemory = 0;   // TotalMemory - UsedMemory at snapshot time.
        ui32 RemainsTasks = 0;    // how many more tasks the node can host (one task == one compute actor, from
                                  // AvailableComputeActors at snapshot time).
        TString DataCenterId;     // for the local-datacenter placement preference.
    };

    struct TStage {
        TStageInfo* Info = nullptr;       // reference to graph-owned stage info (Id, Tasks).
        EStageType Type;                  // may become FIXED if the group leader is FIXED.
        std::optional<TStageIdx> Source;  // group leader (root); empty if this stage is itself a root.
        TGroupIdx Group = 0;              // index into Groups.

        std::list<TStageIdx> Inputs;
        std::list<TStageIdx> Outputs;

        // Task Ids in creation order; the position is the column index. Every stage of a group holds exactly one task
        // per column, so all of them have the same number of tasks (== the group's column count).
        std::vector<ui64> Tasks;
    };

    // Resource cost of one column, summed over the group's stages (one task per stage per column). Uniform across the
    // group's columns - see EstimateTasksResources.
    struct TColumnCost {
        ui64 Memory = 0;   // sum of per-stage TotalMemoryLimit.
        ui32 Tasks = 0;    // number of tasks in the column (one per non-empty member stage).
    };

    // A copy group: a root stage plus the stages that copy from it. Columns are shared by all member stages; the node of
    // a column applies to that column's task in every member stage.
    struct TGroup {
        TStageIdx Root = 0;
        bool Fixed = false;                                // the root is FIXED -> the group's column count is not scaled.
        std::vector<TStageIdx> Stages;                     // member stages, root first.
        std::vector<std::optional<TNodeIdx>> ColumnNodes;  // column index -> node (empty == free, not placed yet).
        TColumnCost ColumnCost;                            // filled by EstimateTasksResources, used by DistributeTasksToNodes.
    };

private:
    size_t NodesCount() const { return NodeIdByIdx.size(); }

    // The basic placement action: pin a column of a group to a node.
    void PlaceColumnOnNode(TGroup& group, size_t columnIdx, TNodeIdx node);

    // Greedy resource-aware placement of all free columns (ported from KqpPlanner's strategy). Pinned columns pre-charge
    // their nodes' budget; free columns are placed largest-memory-first onto the least-loaded node that still fits, using
    // the per-column cost from EstimateTasksResources. Returns false (placing nothing) if some column doesn't fit
    // anywhere - the caller then falls back to round-robin.
    bool DistributeByResources();

    // Fallback placement: every free column round-robin per group, ignoring resources.
    void DistributeRoundRobin();

    // Per-group column distribution over nodes (every column must be placed). This is what the scaling math works on.
    std::vector<TColumnsPerNode> GroupColumns() const;

    bool IsFeasible(const std::vector<TColumnsPerNode>& base, double alpha) const;
    std::vector<TColumnsPerNode> ComputeScaledColumns(const std::vector<TColumnsPerNode>& base, double alpha) const;
    TColumnsPerNode ScaleColumns(const TColumnsPerNode& origin, double alpha) const;
    size_t CountChannelsOnNode(const std::vector<TColumnsPerNode>& columns, TNodeIdx nodeIdx) const;

    void CheckInvariants() const;

private:
    const size_t MaxChannelsCount;
    const TTaskResourceEstimationParams EstimationParams;

    THashMap<TNodeId, TNodeIdx> NodeIds;          // TNodeId -> TNodeIdx
    std::vector<TNodeId> NodeIdByIdx;             // TNodeIdx -> TNodeId (reverse of NodeIds)
    std::vector<TNodeResources> NodeResources;    // TNodeIdx -> per-node budget (parallel to NodeIdByIdx)

    THashMap<TStageId, TStageIdx> StageIds; // TStageId -> TStageIdx
    std::vector<TStage> Stages;             // TStageIdx -> TStage
    std::vector<TGroup> Groups;             // TGroupIdx -> TGroup

    mutable std::vector<TColumnsPerNode> LastFeasible; // last feasible per-group column distribution found by Shrink.
};

} // namespace NKikimr::NKqp
