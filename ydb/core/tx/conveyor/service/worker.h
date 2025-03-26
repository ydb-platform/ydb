#pragma once
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConveyor {

class TWorkerTask {
private:
    YDB_READONLY_DEF(ITask::TPtr, Task);
    YDB_READONLY(TMonotonic, CreateInstant, TMonotonic::Now());
    YDB_READONLY_DEF(std::shared_ptr<TTaskSignals>, TaskSignals);
    std::optional<TMonotonic> StartInstant;
    YDB_READONLY(ui64, ProcessId, 0);
public:
    void OnBeforeStart() {
        StartInstant = TMonotonic::Now();
    }

    TMonotonic GetStartInstant() const {
        Y_ABORT_UNLESS(!!StartInstant);
        return *StartInstant;
    }

    TWorkerTask(const ITask::TPtr& task, const std::shared_ptr<TTaskSignals>& taskSignals, const ui64 processId)
        : Task(task)
        , TaskSignals(taskSignals)
        , ProcessId(processId) {
        Y_ABORT_UNLESS(task);
    }

    bool operator<(const TWorkerTask& wTask) const {
        return Task->GetPriority() < wTask.Task->GetPriority();
    }
};

struct TEvInternal {
    enum EEv {
        EvNewTask = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvTaskProcessedResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        std::vector<TWorkerTask> Tasks;
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
    public:
        TEvNewTask() = default;

        std::vector<TWorkerTask>&& ExtractTasks() {
            return std::move(Tasks);
        }

        explicit TEvNewTask(std::vector<TWorkerTask>&& tasks)
            : Tasks(std::move(tasks)) {
        }
    };

    class TEvTaskProcessedResult:
        public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult> {
    private:
        using TBase = TConclusion<ITask::TPtr>;
        YDB_READONLY_DEF(TDuration, ForwardSendDuration);
        YDB_READONLY_DEF(std::vector<TMonotonic>, Instants);
        YDB_READONLY_DEF(std::vector<ui64>, ProcessIds);
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
    public:
        TEvTaskProcessedResult(std::vector<TMonotonic>&& instants, std::vector<ui64>&& processIds, const TDuration forwardSendDuration)
            : ForwardSendDuration(forwardSendDuration)
            , Instants(std::move(instants))
            , ProcessIds(std::move(processIds)) {
            AFL_VERIFY(ProcessIds.size());
            AFL_VERIFY(Instants.size() == ProcessIds.size() + 1);
        }
    };
};

class TWorker: public NActors::TActorBootstrapped<TWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TWorker>;
    const double CPUUsage = 1;
    bool WaitWakeUp = false;
    std::optional<TDuration> ForwardDuration;
    const NActors::TActorId DistributorId;
    std::vector<TMonotonic> Instants;
    std::vector<ui64> ProcessIds;
    const ::NMonitoring::THistogramPtr SendFwdHistogram;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SendFwdDuration;
    void ExecuteTask(std::vector<TWorkerTask>&& workerTasks);
    void HandleMain(TEvInternal::TEvNewTask::TPtr& ev);
    void HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev);
public:

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInternal::TEvNewTask, HandleMain);
            hFunc(NActors::TEvents::TEvWakeup, HandleMain);
            default:
                ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "unexpected event for task executor: " << ev->GetTypeRewrite();
                break;
        }
    }

    void Bootstrap() {
        Become(&TWorker::StateMain);
    }

    TWorker(const TString& conveyorName, const double cpuUsage, const NActors::TActorId& distributorId, const ::NMonitoring::THistogramPtr sendFwdHistogram, const ::NMonitoring::TDynamicCounters::TCounterPtr sendFwdDuration)
        : TBase("CONVEYOR::" + conveyorName + "::WORKER")
        , CPUUsage(cpuUsage)
        , DistributorId(distributorId)
        , SendFwdHistogram(sendFwdHistogram)
        , SendFwdDuration(sendFwdDuration) {
        AFL_VERIFY(0 < CPUUsage);
        AFL_VERIFY(CPUUsage <= 1);
    }
};

}
