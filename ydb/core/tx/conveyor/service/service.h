#pragma once
#include "worker.h"
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <queue>

namespace NKikimr::NConveyor {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSizeLimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressSize;

    const ::NMonitoring::TDynamicCounters::TCounterPtr AvailableWorkersCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCountLimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SolutionsRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OverlimitRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UseWorkerRate;

    const ::NMonitoring::THistogramPtr WaitingHistogram;
    const ::NMonitoring::THistogramPtr ExecuteHistogram;

    TCounters(const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
        : TBase("Conveyor/" + conveyorName, baseSignals)
        , WaitingQueueSize(TBase::GetValue("WaitingQueueSize"))
        , WaitingQueueSizeLimit(TBase::GetValue("WaitingQueueSizeLimit"))
        , AvailableWorkersCount(TBase::GetValue("AvailableWorkersCount"))
        , WorkersCountLimit(TBase::GetValue("WorkersCountLimit"))
        , IncomingRate(TBase::GetDeriviative("Incoming"))
        , SolutionsRate(TBase::GetDeriviative("Solved"))
        , OverlimitRate(TBase::GetDeriviative("Overlimit"))
        , WaitWorkerRate(TBase::GetDeriviative("WaitWorker"))
        , UseWorkerRate(TBase::GetDeriviative("UseWorker"))
        , WaitingHistogram(TBase::GetHistogram("Waiting", NMonitoring::ExponentialHistogram(20, 2)))
        , ExecuteHistogram(TBase::GetHistogram("Execute", NMonitoring::ExponentialHistogram(20, 2))) {
    }
};

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
        auto result = Tasks.rbegin()->second.front();
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

class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    const TConfig Config;
    const TString ConveyorName = "common";
    TDequePriorityFIFO Waiting;
    std::deque<TActorId> Workers;
    std::optional<NActors::TActorId> SlowWorkerId;
    TCounters Counters;
    THashMap<TString, std::shared_ptr<TTaskSignals>> Signals;

    void HandleMain(TEvExecution::TEvNewTask::TPtr& ev);
    void HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev);

public:

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvNewTask, HandleMain);
            hFunc(TEvInternal::TEvTaskProcessedResult, HandleMain);
            default:
                ALS_ERROR(NKikimrServices::TX_CONVEYOR) << ConveyorName << ": unexpected event for task executor: " << ev->GetTypeRewrite();
                break;
        }
    }

    TDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals);

    void Bootstrap();
};

}
