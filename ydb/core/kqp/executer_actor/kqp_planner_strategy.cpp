#include "kqp_planner_strategy.h"

#include <library/cpp/actors/core/event_pb.h>

#include <util/generic/queue.h>
#include <util/string/builder.h>
#include <util/string/join.h>


namespace NKikimr::NKqp {

namespace {

using namespace NActors;

// Task can allocate extra memory during execution.
// So, we estimate total memory amount required for task as apriori task size multiplied by this constant.
constexpr float TASK_MEMORY_ESTIMATION_OVERFLOW = 1.2f;

class TKqpGreedyPlanner : public IKqpPlannerStrategy {
public:
    ~TKqpGreedyPlanner() override {}

    TVector<TResult> Plan(const TVector<NKikimrKqp::TKqpNodeResources>& nodeResources,
        const TVector<TTaskResourceEstimation>& tasks) override
    {
        TVector<TResult> result;

        struct TNodeDesc {
            ui32 NodeId = std::numeric_limits<ui32>::max();
            TActorId ResourceManagerId;
            ui64 RemainsMemory = 0;
            ui32 RemainsComputeActors = 0;
            TVector<ui64> Tasks;
        };

        struct TComp {
            bool operator ()(const TNodeDesc& l, const TNodeDesc& r) {
                return r.RemainsMemory > l.RemainsMemory && l.RemainsComputeActors > 0;
            }
        };

        TPriorityQueue<TNodeDesc, TVector<TNodeDesc>, TComp> nodes{TComp()};

        for (auto& node : nodeResources) {
            nodes.emplace(TNodeDesc{
                node.GetNodeId(),
                ActorIdFromProto(node.GetResourceManagerActorId()),
                node.GetTotalMemory() - node.GetUsedMemory(),
                node.GetAvailableComputeActors(),
                {}
            });

            if (LogFunc) {
                LogFunc(TStringBuilder() << "[AvailableResources] node #" << node.GetNodeId()
                    << " memory: " << (node.GetTotalMemory() - node.GetUsedMemory())
                    << ", ca: " << node.GetAvailableComputeActors());
            }
        }

        if (LogFunc) {
            for (const auto& task : tasks) {
                LogFunc(TStringBuilder() << "[TaskResources] task: " << task.TaskId << ", memory: " << task.TotalMemoryLimit);
            }
        }

        for (const auto& taskEstimation : tasks) {
            TNodeDesc node = nodes.top();

            if (node.RemainsComputeActors > 0 &&
                node.RemainsMemory > taskEstimation.TotalMemoryLimit * TASK_MEMORY_ESTIMATION_OVERFLOW)
            {
                nodes.pop();
                node.RemainsComputeActors--;
                node.RemainsMemory -= taskEstimation.TotalMemoryLimit * TASK_MEMORY_ESTIMATION_OVERFLOW;
                node.Tasks.push_back(taskEstimation.TaskId);

                if (LogFunc) {
                    LogFunc(TStringBuilder() << "Schedule task: " << taskEstimation.TaskId
                        << " (" << taskEstimation.TotalMemoryLimit << " bytes) "
                        << "to node #" << node.NodeId << ". "
                        << "Remains memory: " << node.RemainsMemory << ", ca: " << node.RemainsComputeActors);
                }

                nodes.emplace(std::move(node));
            } else {
                if (LogFunc) {
                    TStringBuilder err;
                    err << "Not enough resources to execute query. Task " << taskEstimation.TaskId
                        << " (" << taskEstimation.TotalMemoryLimit << " bytes) "
                        << "Node: " << node.NodeId << ", remains memory: " << node.RemainsMemory
                        << ", ca: " << node.RemainsComputeActors;

                    LogFunc(err);
                }

                return result;
            }
        }

        while (!nodes.empty()) {
            TNodeDesc node = nodes.top();
            nodes.pop();

            if (node.Tasks.empty()) {
                continue;
            }

            if (LogFunc) {
                LogFunc(TStringBuilder() << "About to execute tasks [" << JoinSeq(", ", node.Tasks) << "]"
                    << " on node " << node.NodeId);
            }

            result.push_back({});
            TResult& item = result.back();
            item.NodeId = node.NodeId;
            item.ResourceManager = node.ResourceManagerId;
            item.TaskIds.swap(node.Tasks);
        }

        return result;
    }
};

} // anonymous namespace

THolder<IKqpPlannerStrategy> CreateKqpGreedyPlanner() {
    return MakeHolder<TKqpGreedyPlanner>();
}

} // namespace NKikimr::NKqp

