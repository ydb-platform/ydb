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
    YDB_READONLY(double, CPULimit, 1);
    ui64 CPULimitGeneration = 0;
    bool WaitWakeUp = false;
    bool StopRequested = false;
    std::optional<TDuration> ForwardDuration;
    const NActors::TActorId DistributorId;
    const ui64 WorkerIdx;
    const ui64 WorkersPoolId;
    std::optional<TDuration> ExecutionDuration;
    std::vector<TWorkerTaskResult> Results;
    TDuration GetWakeupDuration() const;
    void ExecuteTask(std::vector<TWorkerTask>&& workerTasks);
    void HandleMain(TEvInternal::TEvNewTask::TPtr& ev);
    void HandleMain(TEvInternal::TEvUpdateWorkerCPULimit::TPtr& ev);
    void HandleMain(TEvInternal::TEvRetireWorker::TPtr& ev);
    void HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev);
    void OnWakeup();
    void Stop();

public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInternal::TEvNewTask, HandleMain);
            hFunc(TEvInternal::TEvUpdateWorkerCPULimit, HandleMain);
            hFunc(TEvInternal::TEvRetireWorker, HandleMain);
            hFunc(NActors::TEvents::TEvWakeup, HandleMain);
            default:
            YDB_LOG_ERROR_COMP(NKikimrServices::TX_CONVEYOR, "Unexpected event for task executor",
                {"evType", ev->GetTypeRewrite()});
            break;
        }
    }

    void Bootstrap() {
        Become(&TWorker::StateMain);
    }

    TWorker(const TString& poolName, const double cpuLimit, const NActors::TActorId& distributorId, const ui64 workerIdx,
        const ui64 workersPoolId)
        : TBase("COMPOSITE_CONVEYOR::" + poolName + "::WORKER")
        , CPULimit(cpuLimit)
        , DistributorId(distributorId)
        , WorkerIdx(workerIdx)
        , WorkersPoolId(workersPoolId) {
        AFL_VERIFY(0 < CPULimit && CPULimit <= 1);
    }
};

}   // namespace NKikimr::NConveyorComposite
