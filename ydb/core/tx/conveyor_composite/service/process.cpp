#include "process.h"

#include <algorithm>

namespace NKikimr::NConveyorComposite {

TWorkerTaskPrepare TDequePriorityFIFO::pop(const std::function<bool(const TWorkerTaskPrepare&)>& taskFilter) {
    Y_ABORT_UNLESS(Size);
    for (auto priorityIt = Tasks.end(); priorityIt != Tasks.begin();) {
        --priorityIt;
        auto& tasks = priorityIt->second;
        const auto taskIt = std::find_if(tasks.begin(), tasks.end(), taskFilter);
        if (taskIt == tasks.end()) {
            continue;
        }
        auto result = std::move(*taskIt);
        tasks.erase(taskIt);
        if (tasks.empty()) {
            Tasks.erase(priorityIt);
        }
        --Size;
        return result;
    }
    Y_ABORT("cannot pop a task accepted by the filter");
}

const TWorkerTaskPrepare& TDequePriorityFIFO::top() const {
    Y_ABORT_UNLESS(Size);
    return Tasks.rbegin()->second.front();
}

bool TDequePriorityFIFO::has(const std::function<bool(const TWorkerTaskPrepare&)>& taskFilter) const {
    for (auto priorityIt = Tasks.rbegin(); priorityIt != Tasks.rend(); ++priorityIt) {
        if (std::find_if(priorityIt->second.begin(), priorityIt->second.end(), taskFilter) != priorityIt->second.end()) {
            return true;
        }
    }
    return false;
}

TWorkerTask TProcess::ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& signals,
    const std::function<bool(const TWorkerTaskPrepare&)>& taskFilter) {
    auto result = Tasks.pop(taskFilter);
    CPUUsage->AddPredicted(result.GetPredictedDuration());
    WaitingTasksCount->Dec();
    InProgressTasksCount.Inc();
    const auto taskClass = result.GetTask()->GetTaskClassIdentifier();
    return std::move(result).BuildTask(signals->GetTaskSignals(taskClass));
}

bool TProcess::HasTask(const std::function<bool(const TWorkerTaskPrepare&)>& taskFilter) const {
    return Tasks.has(taskFilter);
}

}
