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
#include <ranges>

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
    template <class TPredicate>
    std::optional<TWorkerTaskPrepare> pop(TPredicate&& predicate) {
        for (auto&& [priority, queue] : Tasks | std::views::reverse) {
            auto filtered = queue | std::views::filter([&](auto& task) {
                return predicate(task);
            });
            auto filteredIt = filtered.begin();
            if (filteredIt == filtered.end()) {
                continue;
            }
            auto result = std::move(*filteredIt);
            queue.erase(filteredIt.base());
            if (queue.empty()) {
                Tasks.erase(priority);
            }
            --Size;
            return result;
        }
        return std::nullopt;
    }
    std::optional<TWorkerTaskPrepare> pop() {
        return pop([](const auto&) {
            return true;
        });
    }
    ui32 size() const {
        return Size;
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

    ui32 GetTasksCount() const {
        return Tasks.size();
    }

    std::optional<TWorkerTask> ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& signals,
        TWorkloadQuotaController& workloadQuota) {
        auto result = Tasks.pop([&](TWorkerTaskPrepare& task) {
            auto reservation = workloadQuota.TryReserve(task.GetWorkloadContext(), task.GetPredictedDuration());
            if (!reservation.Allowed) {
                return false;
            }
            task.SetWorkloadReservation(std::move(reservation.Reservation));
            return true;
        });
        if (!result) {
            return std::nullopt;
        }
        CPUUsage->AddPredicted(result->GetPredictedDuration());
        WaitingTasksCount->Dec();
        InProgressTasksCount.Inc();
        const auto taskClass = result->GetTask()->GetTaskClassIdentifier();
        return std::move(*result).BuildTask(signals->GetTaskSignals(taskClass));
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        CPUUsage->Exchange(result.GetPredictedDuration(), result.GetStart(), result.GetFinish());
        AverageTaskDuration.Add(result.GetDuration());
        InProgressTasksCount.Dec();
    }

    double GetWeight() const {
        return 1.0;
    }

    TProcess(
        const ui64 processId, const std::shared_ptr<TProcessScope>& scope, const std::shared_ptr<TPositiveControlInteger>& waitingTasksCount)
        : ProcessId(processId)
        , Scope(scope)
        , WaitingTasksCount(waitingTasksCount) {
        AFL_VERIFY(WaitingTasksCount);
        CPUUsage = std::make_shared<TCPUUsage>(Scope->GetCPUUsage());
    }

    void RegisterTask(std::shared_ptr<ITask>&& task, const ESpecialTaskCategory category, TWorkloadContext workloadContext) {
        TWorkerTaskPrepare wTask(
            std::move(task), AverageTaskDuration.GetValue(), category, Scope, ProcessId, std::move(workloadContext));
        Tasks.push(std::move(wTask));
        WaitingTasksCount->Inc();
    }
};

}   // namespace NKikimr::NConveyorComposite
