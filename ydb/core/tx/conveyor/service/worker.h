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
public:
    void OnBeforeStart() {
        StartInstant = TMonotonic::Now();
    }

    TMonotonic GetStartInstant() const {
        Y_ABORT_UNLESS(!!StartInstant);
        return *StartInstant;
    }

    TWorkerTask(ITask::TPtr task, std::shared_ptr<TTaskSignals> taskSignals)
        : Task(task)
        , TaskSignals(taskSignals)
    {
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
        TWorkerTask Task;
    public:
        TEvNewTask() = default;

        const TWorkerTask& GetTask() const {
            return Task;
        }

        explicit TEvNewTask(const TWorkerTask& task)
            : Task(task) {
        }
    };

    class TEvTaskProcessedResult:
        public NActors::TEventLocal<TEvTaskProcessedResult, EvTaskProcessedResult> {
    private:
        using TBase = TConclusion<ITask::TPtr>;
        YDB_READONLY_DEF(TMonotonic, StartInstant);
    public:
        TEvTaskProcessedResult(const TWorkerTask& originalTask)
            : StartInstant(originalTask.GetStartInstant()) {

        }
    };
};

class TWorker: public NActors::TActorBootstrapped<TWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TWorker>;
    const double CPUUsage = 1;
    bool WaitWakeUp = false;
    const NActors::TActorId DistributorId;
    std::optional<TWorkerTask> WaitTask;
    void ExecuteTask(const TWorkerTask& workerTask);
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

    TWorker(const TString& conveyorName, const double cpuUsage, const NActors::TActorId& distributorId)
        : TBase("CONVEYOR::" + conveyorName + "::WORKER")
        , CPUUsage(cpuUsage)
        , DistributorId(distributorId)
    {

    }
};

}
