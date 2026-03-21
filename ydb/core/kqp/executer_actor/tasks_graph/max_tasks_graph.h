#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

#include <util/system/types.h>

#include <list>
#include <optional>
#include <vector>

namespace NKikimr::NKqp {

// Reflects the original TKqpTasksGraph using tasks count instead of tasks and links stages with tasks dependencies.
class TMaxTasksGraph {
    using TNodeIdx = size_t;
    using TStageIdx = size_t;

    using TNodeId = ui64;
    using TStageId = NYql::NDq::TStageId;

public:
    using TNodeIdMap = THashMap<TNodeId, TNodeIdx>;
    using TStageIdMap = THashMap<TStageId, TStageIdx>;
    using TTasksPerNode = std::vector<size_t>;

    enum EStageType : int {
        FIXED, // fixed number of tasks
        COPY,  // copies number of tasks from one of previous stages
        ANY,   // any number of tasks
    };

    explicit TMaxTasksGraph(size_t maxChannelsCount);

    void AddNodes(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot);
    void AddStage(const TStageId& stage, EStageType type, const std::list<TStageId>& inputs, std::optional<TStageId> copyInput = std::nullopt);
    void AddTasks(const TStageId& stage, TNodeId node, size_t tasksCount);
    void AddTasks(const TStageId& stage, size_t tasksCount);

    void Shrink(); // TODO: forbid double call - it's not idempotent.

    size_t GetStageTasksCount(const TStageId& stage, TNodeId node) const;
    size_t GetStageTasksCount(const TStageId& stage) const;

private:
    struct TNode {
        size_t MaxChannelsCount;
        size_t TasksCount = 0;
    };

    struct TStage {
        EStageType Type;                 // may become FIXED if group leader is FIXED.
        std::optional<TStageIdx> Source; // source stage for number of tasks - group leader.
        TNodeIdx RoundRobin = 0;         // TODO: use real planning strategy.
    };

private:
    bool IsFeasible(double alpha) const;
    std::vector<TTasksPerNode> ComputeScaledTasks(double alpha) const;
    TTasksPerNode ScaleTasks(TStageIdx stageId, double alpha) const;
    size_t CountChannelsOnNode(const std::vector<TTasksPerNode>& distribution, TNodeIdx nodeId) const;

private:
    const size_t MaxChannelsCount;

    TNodeIdMap NodeIds;
    TStageIdMap StageIds;

    std::vector<TNode> Nodes;                  // TNodeIdx  -> TNode
    std::vector<TStage> Stages;                // TStageIdx -> TStage
    std::vector<std::list<TStageIdx>> Inputs;  // TStageIdx -> inputs
    std::vector<std::list<TStageIdx>> Outputs; // TStageIdx -> outputs

    std::vector<TTasksPerNode> Tasks;  // TStageIdx -> TNodeIdx -> Tasks
    std::vector<size_t> TasksPerStage; // TStageIdx -> Tasks

    mutable std::vector<TTasksPerNode> LastFeasible;
};

} // namespace NKikimr::NKqp
