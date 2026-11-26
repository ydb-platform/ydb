#include "kqp_planner_strategy.h"

#include <ydb/library/actors/core/event_pb.h>

#include <util/generic/queue.h>
#include <util/string/builder.h>
#include <util/string/join.h>


namespace NKikimr::NKqp {

namespace {

using namespace NActors;

// Task can allocate extra memory during execution.
// So, we estimate total memory amount required for task as apriori task size multiplied by this constant.
constexpr float TASK_MEMORY_ESTIMATION_OVERFLOW = 1.2f;

class TNodesManager {
public:
    struct TNodeDesc {
        ui32 NodeId = std::numeric_limits<ui32>::max();
        TActorId ResourceManagerId;
        ui64 RemainsMemory = 0;
        ui32 RemainsComputeActors = 0;
        TVector<ui64> Tasks;
        size_t FixedTaskCount = 0;

        bool operator < (const TNodeDesc& item) const {
            return std::tuple(-i64(FixedTaskCount + Tasks.size()), RemainsMemory, RemainsComputeActors)
                < std::tuple(-i64(item.FixedTaskCount + item.Tasks.size()), item.RemainsMemory, item.RemainsComputeActors);
        }

        std::optional<IKqpPlannerStrategy::TResult> BuildResult() {
            if (Tasks.empty()) {
                return {};
            }
            IKqpPlannerStrategy::TResult item;
            item.NodeId = NodeId;
            item.ResourceManager = ResourceManagerId;
            item.TaskIds.swap(Tasks);
            return item;
        }
    };
private:
    std::vector<TNodeDesc> Nodes;
public:
    std::vector<TNodeDesc>::iterator begin() {
        return Nodes.begin();
    }
    std::vector<TNodeDesc>::iterator end() {
        return Nodes.end();
    }

    std::optional<TNodeDesc> PopNode() {
        if (Nodes.empty()) {
            return {};
        }
        std::pop_heap(Nodes.begin(), Nodes.end());
        auto result = std::move(Nodes.back());
        Nodes.pop_back();
        return result;
    }

    void PushNode(TNodeDesc&& node) {
        Nodes.emplace_back(std::move(node));
        std::push_heap(Nodes.begin(), Nodes.end());
    }

    std::optional<TNodeDesc> PopOptimalNodeWithLimits(const ui64 memoryLimit, const ui32 actorsLimit) {
        std::vector<TNodeDesc> localNodesWithNotEnoughResources;
        std::optional<TNodeDesc> result;
        while (true) {
            if (Nodes.empty()) {
                break;
            }
            std::pop_heap(Nodes.begin(), Nodes.end());
            if (Nodes.back().RemainsComputeActors >= actorsLimit && Nodes.back().RemainsMemory >= memoryLimit) {
                result = std::move(Nodes.back());
                Nodes.pop_back();
                break;
            } else {
                localNodesWithNotEnoughResources.emplace_back(std::move(Nodes.back()));
                Nodes.pop_back();
            }
        }
        for (auto&& i : localNodesWithNotEnoughResources) {
            Nodes.emplace_back(std::move(i));
            std::push_heap(Nodes.begin(), Nodes.end());
        }
        return result;
    }

    TNodesManager(const TVector<const NKikimrKqp::TKqpNodeResources*>& nodeResources,
        const TVector<TTaskResourceEstimation>& tasks, const IKqpPlannerStrategy::TFixedAssignments& fixedAssignments)
    {
        for (auto& node : nodeResources) {
            if (!node->GetAvailableComputeActors()) {
                continue;
            }
            Nodes.emplace_back(TNodeDesc{
                node->GetNodeId(),
                ActorIdFromProto(node->GetResourceManagerActorId()),
                node->GetTotalMemory() - node->GetUsedMemory(),
                node->GetAvailableComputeActors(),
                {}
                });
        }
        if (!fixedAssignments.empty()) {
            // Fixed tasks on nodes outside this pass's candidates stay there; they do not spend a candidate's budget.
            THashMap<ui64, TNodeDesc*> nodesById;
            nodesById.reserve(Nodes.size());
            for (auto& node : Nodes) {
                nodesById.emplace(node.NodeId, &node);
            }
            for (const auto& task : tasks) {
                if (const auto* nodeId = fixedAssignments.FindPtr(task.TaskId)) {
                    if (const auto* entry = nodesById.FindPtr(*nodeId)) {
                        auto& node = **entry;
                        const ui64 memory = task.TotalMemoryLimit * TASK_MEMORY_ESTIMATION_OVERFLOW;
                        node.RemainsMemory -= Min(node.RemainsMemory, memory);
                        if (node.RemainsComputeActors) {
                            --node.RemainsComputeActors;
                        }
                        ++node.FixedTaskCount;
                    }
                }
            }
        }
        std::make_heap(Nodes.begin(), Nodes.end());
    }
};

class TKqpGreedyPlanner : public IKqpPlannerStrategy {
public:
    ~TKqpGreedyPlanner() override {}

    std::optional<TVector<TResult>> Plan(const TVector<const NKikimrKqp::TKqpNodeResources*>& nodeResources,
        const TVector<TTaskResourceEstimation>& tasks, const TFixedAssignments& fixedAssignments) override
    {
        TVector<TResult> result;
        TNodesManager nodes(nodeResources, tasks, fixedAssignments);

        for (const auto& taskEstimation : tasks) {
            if (fixedAssignments.contains(taskEstimation.TaskId)) {
                continue;
            }
            const ui64 memory = taskEstimation.TotalMemoryLimit * TASK_MEMORY_ESTIMATION_OVERFLOW;
            auto node = nodes.PopOptimalNodeWithLimits(memory, 1);
            if (!node) {
                if (LogFunc) {
                    TStringBuilder err;
                    err << "Not enough resources to execute query. Task " << taskEstimation.TaskId
                        << " (" << taskEstimation.TotalMemoryLimit << " bytes) ";

                    LogFunc(err);
                }
                return std::nullopt;
            } else {
                if (LogFunc) {
                    LogFunc(TStringBuilder() << "Schedule task: " << taskEstimation.TaskId
                        << " (" << taskEstimation.TotalMemoryLimit << " bytes) "
                        << "to node #" << node->NodeId << ". "
                        << "Remains memory: " << node->RemainsMemory << ", ca: " << node->RemainsComputeActors);
                }
                node->RemainsMemory -= memory;
                node->Tasks.emplace_back(taskEstimation.TaskId);
                --node->RemainsComputeActors;
                nodes.PushNode(std::move(*node));
            }
        }

        while (auto node = nodes.PopNode()) {
            auto resultNode = node->BuildResult();
            if (resultNode) {
                if (LogFunc) {
                    LogFunc(TStringBuilder() << "About to execute tasks [" << JoinSeq(", ", resultNode->TaskIds) << "]"
                        << " on node " << resultNode->NodeId);
                }
                result.emplace_back(std::move(*resultNode));
            }
        }

        return result;
    }
};

class TKqpMockEmptyPlanner : public IKqpPlannerStrategy {
public:
    ~TKqpMockEmptyPlanner() override {}

    std::optional<TVector<TResult>> Plan(const TVector<const NKikimrKqp::TKqpNodeResources*>&,
        const TVector<TTaskResourceEstimation>&, const TFixedAssignments&) override
    {
        return std::nullopt;
    }
};

} // anonymous namespace

THolder<IKqpPlannerStrategy> CreateKqpGreedyPlanner() {
    return MakeHolder<TKqpGreedyPlanner>();
}

THolder<IKqpPlannerStrategy> CreateKqpMockEmptyPlanner() {
    return MakeHolder<TKqpMockEmptyPlanner>();
}

} // namespace NKikimr::NKqp
