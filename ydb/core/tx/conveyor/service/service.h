#pragma once
#include "worker.h"
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/library/signals/owner.h>
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
    const ::NMonitoring::TDynamicCounters::TCounterPtr AmountCPULimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SolutionsRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OverlimitRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UseWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ChangeCPULimitRate;

    const ::NMonitoring::THistogramPtr WaitingHistogram;
    const ::NMonitoring::THistogramPtr PackHistogram;
    const ::NMonitoring::THistogramPtr PackExecuteHistogram;
    const ::NMonitoring::THistogramPtr TaskExecuteHistogram;
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
        , AmountCPULimit(TBase::GetValue("AmountCPULimit"))
        , IncomingRate(TBase::GetDeriviative("Incoming"))
        , SolutionsRate(TBase::GetDeriviative("Solved"))
        , OverlimitRate(TBase::GetDeriviative("Overlimit"))
        , WaitWorkerRate(TBase::GetDeriviative("WaitWorker"))
        , UseWorkerRate(TBase::GetDeriviative("UseWorker"))
        , ChangeCPULimitRate(TBase::GetDeriviative("ChangeCPULimit"))
        , WaitingHistogram(TBase::GetHistogram("Waiting/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , PackHistogram(TBase::GetHistogram("ExecutionPack/Count", NMonitoring::LinearHistogram(25, 1, 1)))
        , PackExecuteHistogram(TBase::GetHistogram("PackExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , TaskExecuteHistogram(TBase::GetHistogram("TaskExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
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

class TCPUGroup {
    YDB_READONLY_DEF(TString, Name);
    YDB_ACCESSOR_DEF(double, CPUThreadsLimit);
    TPositiveControlInteger ProcessesCount;
public:
    using TPtr = std::shared_ptr<TCPUGroup>;

    TCPUGroup(const TString& name, const double cpuThreadsLimit)
        : Name(name)
        , CPUThreadsLimit(cpuThreadsLimit) {
    }

    ~TCPUGroup() {
        AFL_VERIFY(ProcessesCount == 0);
    }

    bool DecProcesses() {
        --ProcessesCount;
        return ProcessesCount == 0;
    }

    void IncProcesses() {
        ++ProcessesCount;
    }
};

class TProcess {
private:
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, CPUTime, 0);
    YDB_READONLY_DEF(TCPUGroup::TPtr, CPUGroup);
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

    TProcess(const ui64 processId, TCPUGroup::TPtr cpuGroup)
        : ProcessId(processId)
        , CPUGroup(std::move(cpuGroup)) {
        AFL_VERIFY(CPUGroup);
        IncRegistration();
    }

    void AddCPUTime(const TDuration d) {
        CPUTime += d.MicroSeconds();
    }

    TProcessOrdered GetAddress(const double amountCPULimit) const {
        return TProcessOrdered(ProcessId, CPUTime * std::max<double>(1, amountCPULimit - CPUGroup->GetCPUThreadsLimit()));
    }
};

class TWorkersPool {
    class TWorkerInfo {
        YDB_READONLY(bool, RunningTask, false);
        YDB_READONLY(double, CPUSoftLimit, 0.0);
        YDB_READONLY_DEF(TActorId, WorkerId);
    public:
        TWorkerInfo(const double cpuSoftLimit, std::unique_ptr<TWorker> worker)
            : CPUSoftLimit(cpuSoftLimit)
            , WorkerId(TActivationContext::Register(worker.release())) {
        }

        void ChangeCPUSoftLimit(const double newCPUSoftLimit) {
            if (std::abs(newCPUSoftLimit - CPUSoftLimit) > Eps) {
                TActivationContext::Send(WorkerId, std::make_unique<TEvInternal::TEvChangeCPUSoftLimit>(newCPUSoftLimit));
                CPUSoftLimit = newCPUSoftLimit;
            }
        }

        void OnStartTask() {
            AFL_VERIFY(!RunningTask);
            RunningTask = true;
        }

        void OnStopTask() {
            AFL_VERIFY(RunningTask);
            RunningTask = false;
        }
    };

    YDB_READONLY(ui32, WorkersCount, 0);
    YDB_READONLY(double, MaxWorkerThreads, 0);
    YDB_READONLY(double, AmountCPULimit, 0);
    ui32 ActiveWorkersCount = 0;
    double ActiveWorkerThreads = 0.0;
    std::vector<TWorkerInfo> Workers;
    std::vector<ui32> ActiveWorkersIdx;
    TCounters Counters;

public:
    static constexpr double Eps = 1e-6;

    using TPtr = std::shared_ptr<TWorkersPool>;

    TWorkersPool(const TString& conveyorName, const NActors::TActorId& distributorId, const TConfig& config, const TCounters& counters);

    bool HasFreeWorker() const;

    void RunTask(std::vector<TWorkerTask>&& tasksBatch);

    void ReleaseWorker(const ui32 workerIdx);

    void ChangeAmountCPULimit(const double delta);
};

class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    const TConfig Config;
    const TString ConveyorName = "common";
    const bool EnableProcesses = false;
    TPositiveControlInteger WaitingTasksCount;
    THashMap<ui64, TProcess> Processes;
    std::set<TProcessOrdered> ProcessesOrdered;
    TWorkersPool::TPtr WorkersPool;
    THashMap<TString, TCPUGroup::TPtr> CPUGroups;
    TCounters Counters;
    THashMap<TString, std::shared_ptr<TTaskSignals>> Signals;
    TMonotonic LastAddProcessInstant = TMonotonic::Now();

    void HandleMain(TEvExecution::TEvNewTask::TPtr& ev);
    void HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev);
    void HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev);
    void HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev);

    void AddProcess(const ui64 processId, const TCPULimitsConfig& cpuLimits);

    void AddCPUTime(const ui64 processId, const TDuration d);

    TWorkerTask PopTask();

    void PushTask(const TWorkerTask& task);

    void ChangeAmountCPULimit(const double delta);

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

    TDistributor(const TConfig& config, const TString& conveyorName, const bool enableProcesses, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals);

    ~TDistributor();

    void Bootstrap();
};

}
