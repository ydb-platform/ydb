#pragma once
#include "behaviour.h"
#include "config.h"
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/common.h>
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/bg_tasks/service.h>
#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/ds_table/service.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NBackgroundTasks {

class TEvLockPingerFinished: public TEventLocal<TEvLockPingerFinished, EEvents::EvLockPingerFinished> {

};

class TEvLockPingerStart: public TEventLocal<TEvLockPingerStart, EEvents::EvLockPingerStart> {

};

class TEvStartAssign: public TEventLocal<TEvStartAssign, EEvents::EvStartAssign> {

};

class TEvAssignFinished: public TEventLocal<TEvAssignFinished, EEvents::EvAssignFinished> {

};

class TEvFetchingFinished: public TEventLocal<TEvFetchingFinished, EEvents::EvFetchingFinished> {

};

class TEvTaskFetched: public TEventLocal<TEvTaskFetched, EEvents::EvTaskFetched> {
private:
    YDB_READONLY_DEF(TTask, Task);
public:
    TEvTaskFetched(const TTask& task)
        : Task(task)
    {

    }
};

class TEvTaskExecutorFinished: public TEventLocal<TEvTaskExecutorFinished, EEvents::EvTaskExecutorFinished> {
private:
    YDB_READONLY_DEF(TString, TaskId);
public:
    TEvTaskExecutorFinished(const TString& taskId)
        : TaskId(taskId)
    {

    }
};

class TExecutor: public NActors::TActorBootstrapped<TExecutor> {
private:
    using TBase = NActors::TActorBootstrapped<TExecutor>;
    TString TableName;
    const TString ExecutorId = TGUID::CreateTimebased().AsUuidString();
    const TConfig Config;
    std::set<TString> CurrentTaskIds;
    TExecutorController::TPtr InternalController;
    NMetadata::NProvider::TEventsWaiter DeferredEventsOnIntialization;

    std::shared_ptr<TBehaviour> Behaviour;

    enum class EActivity {
        Created,
        Preparation,
        Active
    };

    EActivity ActivityState = EActivity::Created;

    bool CheckActivity() {
        switch (ActivityState) {
            case EActivity::Created:
                ActivityState = EActivity::Preparation;
                Sender<NMetadata::NProvider::TEvPrepareManager>(Behaviour).SendTo(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()));
                break;
            case EActivity::Preparation:
                break;
            case EActivity::Active:
                return true;
        }
        return false;
    }

protected:
    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr& ev);
    void Handle(TEvStartAssign::TPtr& ev);
    void Handle(TEvAssignFinished::TPtr& ev);
    void Handle(TEvFetchingFinished::TPtr& ev);
    void Handle(TEvTaskFetched::TPtr& ev);
    void Handle(TEvTaskExecutorFinished::TPtr& ev);
    void Handle(TEvAddTask::TPtr& ev);
    void Handle(TEvUpdateTaskEnabled::TPtr& ev);
    void Handle(TEvLockPingerStart::TPtr& ev);
    void Handle(TEvLockPingerFinished::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            hFunc(TEvStartAssign, Handle);
            hFunc(TEvAssignFinished, Handle);
            hFunc(TEvFetchingFinished, Handle);
            hFunc(TEvTaskFetched, Handle);
            hFunc(TEvAddTask, Handle);
            hFunc(TEvTaskExecutorFinished, Handle);
            hFunc(TEvLockPingerStart, Handle);
            hFunc(TEvLockPingerFinished, Handle);
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }

public:
    void Bootstrap();

    TExecutor(const TConfig& config)
        : Config(config)
        , Behaviour(std::make_shared<TBehaviour>(Config))
    {

        TServiceOperator::Register();
    }
};

IActor* CreateService(const TConfig& config);

}
