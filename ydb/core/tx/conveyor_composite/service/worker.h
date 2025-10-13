#pragma once

#include "counters.h"
#include "events.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NConveyorComposite {

class TWorker: public NActors::TActorBootstrapped<TWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TWorker>;
    const double CPUHardLimit = 1;
    YDB_READONLY(double, CPUSoftLimit, 1);
    ui64 CPULimitGeneration = 0;
    bool WaitWakeUp = false;
    std::optional<TDuration> ForwardDuration;
    const NActors::TActorId DistributorId;
    const ui64 WorkerIdx;
    const ui64 WorkersPoolId;
    std::optional<TDuration> ExecutionDuration;
    std::vector<TWorkerTaskResult> Results;
    TDuration GetWakeupDuration() const;
    void ExecuteTask(std::vector<TWorkerTask>&& workerTasks);
    void HandleMain(TEvInternal::TEvNewTask::TPtr& ev);
    void HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev);
    void OnWakeup();

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

    TWorker(const TString& poolName, const double cpuHardLimit, const NActors::TActorId& distributorId, const ui64 workerIdx,
        const ui64 workersPoolId)
        : TBase("COMPOSITE_CONVEYOR::" + poolName + "::WORKER")
        , CPUHardLimit(cpuHardLimit)
        , CPUSoftLimit(cpuHardLimit)
        , DistributorId(distributorId)
        , WorkerIdx(workerIdx)
        , WorkersPoolId(workersPoolId) {
        AFL_VERIFY(0 < CPUHardLimit);
        AFL_VERIFY(CPUHardLimit <= 1);
    }
};

}   // namespace NKikimr::NConveyorComposite
