#pragma once
#include "worker.h"
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <queue>

namespace NKikimr::NConveyor {

class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    const TConfig Config;
    const TString ConveyorName = "common";
    std::vector<TActorId> Workers;
    std::priority_queue<TWorkerTask> Waiting;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSizeLimit;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCountLimit;
    const ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SolutionsRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OverlimitRate;

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

NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals);

}
