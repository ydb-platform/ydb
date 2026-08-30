#pragma once
#include "counters.h"
#include "events.h"

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/kqp/runtime/scheduler/fwd.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>
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

    NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr PendingConfigNotification;
    NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr QueuedConfigNotification;
    NKqp::NScheduler::TComputeSchedulerPtr Scheduler;
    std::optional<TMonotonic> ScheduledWorkloadQuotaWakeupAt;

    void HandleMain(TEvExecution::TEvNewTask::TPtr& ev);
    void HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev);
    void HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev);
    void HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev);
    void HandleMain(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void HandleMain(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void HandleMain(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void HandleMain(TEvInternal::TEvRetryConfigSubscription::TPtr& ev);
    void HandleMain(TEvInternal::TEvWorkloadQuotaWakeup::TPtr& ev);

    void SubscribeToCompositeConveyorConfig();
    void ScheduleConfigSubscriptionRetry();
    void ReplyConfigNotification(const NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void CompleteConfigUpdate();
    void DrainTasks();
    void ScheduleWorkloadQuotaWakeup(TMonotonic deadline);

public:
    STATEFN(StateMain) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("name", ConveyorName)
        //            ("workers", Workers.size())("waiting", Waiting.size())("actor_id", SelfId());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvNewTask, HandleMain);
            hFunc(TEvInternal::TEvTaskProcessedResult, HandleMain);
            hFunc(TEvExecution::TEvRegisterProcess, HandleMain);
            hFunc(TEvExecution::TEvUnregisterProcess, HandleMain);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleMain);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleMain);
            hFunc(NActors::TEvents::TEvUndelivered, HandleMain);
            hFunc(TEvInternal::TEvRetryConfigSubscription, HandleMain);
            hFunc(TEvInternal::TEvWorkloadQuotaWakeup, HandleMain);
            default:
                YDB_LOG_ERROR_COMP(NKikimrServices::TX_CONVEYOR, "",
                    {"problem", "unexpected event for task executor"},
                    {"evType", ev->GetTypeName()});
                break;
        }
    }

    TDistributor(const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals,
        NKqp::NScheduler::TComputeSchedulerPtr scheduler);

    void Bootstrap();
};

inline NActors::IActor* CreateService(
    const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals,
    NKqp::NScheduler::TComputeSchedulerPtr scheduler = {}) {
    TServiceOperator::Register(config);
    return new TDistributor(config, conveyorSignals, std::move(scheduler));
}

}   // namespace NKikimr::NConveyorComposite
