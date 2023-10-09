#include "add_tasks.h"
#include "assign_tasks.h"
#include "executor.h"
#include "lock_pinger.h"
#include "task_executor.h"
#include "task_enabled.h"
#include "fetch_tasks.h"
#include "initialization.h"

#include <ydb/services/metadata/initializer/fetcher.h>
#include <ydb/services/metadata/initializer/manager.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NBackgroundTasks {

void TExecutor::Handle(TEvStartAssign::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "start assign";
    if (Config.GetMaxInFlight() > CurrentTaskIds.size()) {
        Register(new TAssignTasksActor(Config.GetMaxInFlight() - CurrentTaskIds.size(), InternalController, ExecutorId));
    }
}

void TExecutor::Handle(TEvAssignFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "assign finished";
    Register(new TFetchTasksActor(CurrentTaskIds, ExecutorId, InternalController));
}

void TExecutor::Handle(TEvFetchingFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "assign scheduled: " << Config.GetPullPeriod();
    Schedule(Config.GetPullPeriod(), new TEvStartAssign);
}

void TExecutor::Handle(TEvLockPingerFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "pinger scheduled: " << Config.GetPingPeriod();
    Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
}

void TExecutor::Handle(TEvLockPingerStart::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "pinger start";
    if (CurrentTaskIds.size()) {
        Register(new TLockPingerActor(InternalController, CurrentTaskIds));
    } else {
        Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
    }
}

void TExecutor::Handle(TEvTaskFetched::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "task fetched";
    if (CurrentTaskIds.emplace(ev->Get()->GetTask().GetId()).second) {
        Register(new TTaskExecutor(ev->Get()->GetTask(), InternalController));
    }
}

void TExecutor::Handle(TEvTaskExecutorFinished::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "task executor finished";
    Y_ABORT_UNLESS(CurrentTaskIds.contains(ev->Get()->GetTaskId()));
    CurrentTaskIds.erase(ev->Get()->GetTaskId());
    Sender<TEvStartAssign>().SendTo(SelfId());
}

void TExecutor::Handle(TEvAddTask::TPtr& ev) {
    if (CheckActivity()) {
        ALS_DEBUG(NKikimrServices::BG_TASKS) << "add task";
        Register(new TAddTasksActor(InternalController, ev->Get()->GetTask(), ev->Sender));
    } else {
        DeferredEventsOnIntialization.Add(*ev);
    }
}

void TExecutor::Handle(TEvUpdateTaskEnabled::TPtr& ev) {
    if (CheckActivity()) {
        ALS_DEBUG(NKikimrServices::BG_TASKS) << "start task";
        Register(new TUpdateTaskEnabledActor(InternalController, ev->Get()->GetTaskId(), ev->Get()->GetEnabled(), ev->Sender));
    } else {
        DeferredEventsOnIntialization.Add(*ev);
    }
}

void TExecutor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr& /*ev*/) {
    ActivityState = EActivity::Active;
    Sender<TEvStartAssign>().SendTo(SelfId());
    Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
    DeferredEventsOnIntialization.ResendAll(SelfId());
}

void TExecutor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto snapshot = ev->Get()->GetValidatedSnapshotAs<NMetadata::NInitializer::TSnapshot>();
    Y_ABORT_UNLESS(snapshot, "incorrect initialization snapshot from metadata service");
    if (snapshot->HasComponent("bg_tasks")) {
        CheckActivity();
    }
}

void TExecutor::Bootstrap() {
    InternalController = std::make_shared<TExecutorController>(SelfId(), Config);
    Become(&TExecutor::StateMain);
    auto manager = std::make_shared<NMetadata::NInitializer::TFetcher>();
    Y_ABORT_UNLESS(NMetadata::NProvider::TServiceOperator::IsEnabled(), "metadata service not active");
    Sender<NMetadata::NProvider::TEvSubscribeExternal>(manager).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
}

NActors::IActor* CreateService(const TConfig& config) {
    return new TExecutor(config);
}

}
