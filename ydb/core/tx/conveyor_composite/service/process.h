#pragma once
#include "common.h"
#include "worker.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <queue>

namespace NKikimr::NConveyorComposite {

class TDequePriorityFIFO {
private:
    std::map<ui32, std::deque<TWorkerTask>> Tasks;
    ui32 Size = 0;

public:
    void push(const TWorkerTask& task) {
        Tasks[(ui32)task.GetTask()->GetPriority()].emplace_back(task);
        ++Size;
    }
    TWorkerTask pop() {
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

class TProcess: public TMoveOnly {
private:
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    YDB_ACCESSOR_DEF(TDequePriorityFIFO, Tasks);
    TAverageCalcer<TDuration> AverageTaskDuration;
    ui32 LinksCount = 0;

public:
    void DoQuant(const TMonotonic newStart) {
        CPUUsage->Cut(newStart);
    }

    bool HasTasks() const {
        return Tasks.size();
    }

    TWorkerTask ExtractTaskWithPrediction() {
        auto result = Tasks.pop();
        CPUUsage->AddPredicted(result.GetPredictedDuration());
        return result;
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        CPUUsage->Exchange(result.GetPredictedDuration(), result.GetStart(), result.GetFinish());
        AverageTaskDuration.Add(result.GetDuration());
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

    TProcess(const ui64 processId, const std::shared_ptr<TCPUUsage>& scopeUsage)
        : ProcessId(processId)
    {
        CPUUsage = std::make_shared<TCPUUsage>(scopeUsage);
        IncRegistration();
    }

    void RegisterTask(const std::shared_ptr<ITask>& task, const TString& scopeId, const std::shared_ptr<TCategorySignals>& signals) {
        TWorkerTask wTask(task, AverageTaskDuration.GetValue(), signals->GetCategory(), scopeId,
            signals->GetTaskSignals(task->GetTaskClassIdentifier()), ProcessId);
        Tasks.push(std::move(wTask));
    }
};

}   // namespace NKikimr::NConveyorComposite
