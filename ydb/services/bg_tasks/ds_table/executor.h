#pragma once
#include "config.h"
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/common.h>
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/bg_tasks/service.h>
#include <ydb/services/metadata/initializer/accessor_init.h>

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
protected:
    void Handle(NMetadataInitializer::TEvInitializationFinished::TPtr& ev);
    void Handle(TEvStartAssign::TPtr& ev);
    void Handle(TEvAssignFinished::TPtr& ev);
    void Handle(TEvFetchingFinished::TPtr& ev);
    void Handle(TEvTaskFetched::TPtr& ev);
    void Handle(TEvTaskExecutorFinished::TPtr& ev);
    void Handle(TEvAddTask::TPtr& ev);
    void Handle(TEvUpdateTaskEnabled::TPtr& ev);
    void Handle(TEvLockPingerStart::TPtr& ev);
    void Handle(TEvLockPingerFinished::TPtr& ev);
    void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadataInitializer::TEvInitializationFinished, Handle);
            hFunc(TEvStartAssign, Handle);
            hFunc(TEvAssignFinished, Handle);
            hFunc(TEvFetchingFinished, Handle);
            hFunc(TEvTaskFetched, Handle);
            hFunc(TEvAddTask, Handle);
            hFunc(TEvTaskExecutorFinished, Handle);
            hFunc(TEvLockPingerStart, Handle);
            hFunc(TEvLockPingerFinished, Handle);
            hFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }

public:
    void Bootstrap();

    TExecutor(const TConfig& config)
        : Config(config)
    {
        TServiceOperator::Register();
    }
};

IActor* CreateService(const TConfig& config);

}
