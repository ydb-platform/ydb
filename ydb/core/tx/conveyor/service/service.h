#pragma once
#include "worker.h"
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <queue>

namespace NKikimr::NConveyor {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr ProcessesCount;

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
    const ::NMonitoring::THistogramPtr PackHistogram;
    const ::NMonitoring::THistogramPtr ExecuteHistogram;
    const ::NMonitoring::THistogramPtr SendBackHistogram;
    const ::NMonitoring::THistogramPtr SendFwdHistogram;
    const ::NMonitoring::THistogramPtr ReceiveTaskHistogram;

    const ::NMonitoring::TDynamicCounters::TCounterPtr SendBackDuration;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SendFwdDuration;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ExecuteDuration;

    TCounters(const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
        : TBase("Conveyor/" + conveyorName, baseSignals)
        , ProcessesCount(TBase::GetValue("Processes/Count"))
        , WaitingQueueSize(TBase::GetValue("WaitingQueueSize"))
        , WaitingQueueSizeLimit(TBase::GetValue("WaitingQueueSizeLimit"))
        , AvailableWorkersCount(TBase::GetValue("AvailableWorkersCount"))
        , WorkersCountLimit(TBase::GetValue("WorkersCountLimit"))
        , IncomingRate(TBase::GetDeriviative("Incoming"))
        , SolutionsRate(TBase::GetDeriviative("Solved"))
        , OverlimitRate(TBase::GetDeriviative("Overlimit"))
        , WaitWorkerRate(TBase::GetDeriviative("WaitWorker"))
        , UseWorkerRate(TBase::GetDeriviative("UseWorker"))
        , WaitingHistogram(TBase::GetHistogram("Waiting/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , PackHistogram(TBase::GetHistogram("ExecutionPack/Count", NMonitoring::LinearHistogram(25, 1, 1)))
        , ExecuteHistogram(TBase::GetHistogram("Execute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , SendBackHistogram(TBase::GetHistogram("SendBack/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , SendFwdHistogram(TBase::GetHistogram("SendForward/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , ReceiveTaskHistogram(TBase::GetHistogram("ReceiveTask/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , SendBackDuration(TBase::GetDeriviative("SendBack/Duration/Us"))
        , SendFwdDuration(TBase::GetDeriviative("SendForward/Duration/Us"))
        , ExecuteDuration(TBase::GetDeriviative("Execute/Duration/Us"))
    {
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

class TProcess {
private:
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, CPUTime, 0);
    YDB_ACCESSOR_DEF(TDequePriorityFIFO, Tasks);
    ui32 LinksCount = 0;
public:
    void CleanCPUMetric() {
        CPUTime = 0;
    }

    bool DecRegistration() {
        AFL_VERIFY(LinksCount);
        --LinksCount;
        return LinksCount == 0;
    }

    void IncRegistration() {
        ++LinksCount;
    }

    TProcess(const ui64 processId)
        : ProcessId(processId) {
        IncRegistration();
    }

    void AddCPUTime(const TDuration d) {
        CPUTime += d.MicroSeconds();
    }

    TProcessOrdered GetAddress() const {
        return TProcessOrdered(ProcessId, CPUTime);
    }
};

class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    ui32 WorkersCount = 0;
    const TConfig Config;
    const TString ConveyorName = "common";
    TPositiveControlInteger WaitingTasksCount;
    THashMap<ui64, TProcess> Processes;
    std::set<TProcessOrdered> ProcessesOrdered;
    std::deque<TActorId> Workers;
    std::optional<NActors::TActorId> SlowWorkerId;
    TCounters Counters;
    THashMap<TString, std::shared_ptr<TTaskSignals>> Signals;
    TMonotonic LastAddProcessInstant = TMonotonic::Now();

    void HandleMain(TEvExecution::TEvNewTask::TPtr& ev);
    void HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev);
    void HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev);
    void HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev);

    void AddProcess(const ui64 processId);

    void AddCPUTime(const ui64 processId, const TDuration d);

    TWorkerTask PopTask();

    void PushTask(const TWorkerTask& task);

public:

    STATEFN(StateMain) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("name", ConveyorName)
        //            ("workers", Workers.size())("waiting", Waiting.size())("actor_id", SelfId());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvNewTask, HandleMain);
            hFunc(TEvInternal::TEvTaskProcessedResult, HandleMain);
            hFunc(TEvExecution::TEvRegisterProcess, HandleMain);
            hFunc(TEvExecution::TEvUnregisterProcess, HandleMain);
            default:
                AFL_ERROR(NKikimrServices::TX_CONVEYOR)("problem", "unexpected event for task executor")("ev_type", ev->GetTypeName());
                break;
        }
    }

    TDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals);

    void Bootstrap();
};

}
