#pragma once
#include "common.h"
#include "scope.h"
#include "worker.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/object_counter.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <queue>

namespace NKikimr::NConveyorComposite {

class TDequePriorityFIFO {
private:
    std::map<ui32, std::deque<TWorkerTaskPrepare>> Tasks;
    ui32 Size = 0;

public:
    void push(TWorkerTaskPrepare&& task) {
        auto priority = (ui32)task.GetTask()->GetPriority();
        Tasks[priority].emplace_back(std::move(task));
        ++Size;
    }
    TWorkerTaskPrepare pop() {
        Y_ABORT_UNLESS(Size);
        auto result = std::move(Tasks.rbegin()->second.front());
        Tasks.rbegin()->second.pop_front();
        if (Tasks.rbegin()->second.size() == 0) {
            Tasks.erase(--Tasks.end());
        }
        --Size;
        return result;
    }
    ui32 size() const {
        return Size;
    }
};

class TProcessOrdered {
private:
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, CPUTime, 0);

public:
    TProcessOrdered(const ui64 processId, const ui64 cpuTime)
        : ProcessId(processId)
        , CPUTime(cpuTime) {
    }

    bool operator<(const TProcessOrdered& item) const {
        if (CPUTime < item.CPUTime) {
            return true;
        }
        if (item.CPUTime < CPUTime) {
            return false;
        }
        return ProcessId < item.ProcessId;
    }
};

class TProcess: public TNonCopyable, public NColumnShard::TMonitoringObjectsCounter<TProcess> {
private:
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    YDB_ACCESSOR_DEF(TDequePriorityFIFO, Tasks);
    YDB_READONLY_DEF(std::shared_ptr<TProcessScope>, Scope);

    std::shared_ptr<TPositiveControlInteger> WaitingTasksCount;
    TPositiveControlInteger InProgressTasksCount;
    TAverageCalcer<TDuration> AverageTaskDuration;
    ui32 LinksCount = 0;
    TDuration BaseWeight = TDuration::Zero();

public:
    ui32 GetInProgressTasksCount() const {
        return InProgressTasksCount.Val();
    }

    void SetBaseWeight(const TDuration d) {
        BaseWeight = d;
        CPUUsage->Clear();
        AFL_VERIFY(InProgressTasksCount.Val() == 0);
        AFL_VERIFY(Tasks.size() == 0);
    }

    TDuration GetWeightedUsage() const {
        return BaseWeight + CPUUsage->CalcWeight(GetWeight());
    }

    ~TProcess() {
        WaitingTasksCount->Sub(Tasks.size());
    }

    bool HasTasks() const {
        return Tasks.size();
    }

    ui32 GetTasksCount() const {
        return Tasks.size();
    }

    TWorkerTask ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& signals) {
        auto result = Tasks.pop();
        CPUUsage->AddPredicted(result.GetPredictedDuration());
        WaitingTasksCount->Dec();
        InProgressTasksCount.Inc();
        return std::move(result).BuildTask(signals->GetTaskSignals(result.GetTask()->GetTaskClassIdentifier()));
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        CPUUsage->Exchange(result.GetPredictedDuration(), result.GetStart(), result.GetFinish());
        AverageTaskDuration.Add(result.GetDuration());
        InProgressTasksCount.Dec();
    }

    [[nodiscard]] bool DecRegistration() {
        AFL_VERIFY(LinksCount);
        --LinksCount;
        return LinksCount == 0;
    }

    double GetWeight() const {
        return 1.0;
    }

    void IncRegistration() {
        ++LinksCount;
    }

    TProcess(
        const ui64 processId, const std::shared_ptr<TProcessScope>& scope, const std::shared_ptr<TPositiveControlInteger>& waitingTasksCount)
        : ProcessId(processId)
        , Scope(scope)
        , WaitingTasksCount(waitingTasksCount) {
        AFL_VERIFY(WaitingTasksCount);
        CPUUsage = std::make_shared<TCPUUsage>(Scope->GetCPUUsage());
        IncRegistration();
    }

    void RegisterTask(const std::shared_ptr<ITask>& task, const ESpecialTaskCategory category) {
        TWorkerTaskPrepare wTask(task, AverageTaskDuration.GetValue(), category, Scope, ProcessId);
        Tasks.push(std::move(wTask));
        WaitingTasksCount->Inc();
    }
};

}   // namespace NKikimr::NConveyorComposite
