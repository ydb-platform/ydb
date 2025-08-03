#pragma once
#include "counters.h"
#include "events.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <queue>

namespace NKikimr::NConveyorComposite {
class TTasksManager;
class TDistributor: public TActorBootstrapped<TDistributor> {
private:
    using TBase = TActorBootstrapped<TDistributor>;
    const NConfig::TConfig Config;
    const TString ConveyorName = "common";
    std::shared_ptr<TTasksManager> Manager;
    TCounters Counters;
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

    TDistributor(const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals);

    ~TDistributor();

    void Bootstrap();
};

}   // namespace NKikimr::NConveyorComposite
