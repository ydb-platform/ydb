#pragma once
#include "config.h"

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>

namespace NKikimr::NBackgroundTasks {

class TTask;

class TExecutorController {
private:
    const NActors::TActorIdentity ExecutorActorId;
    YDB_READONLY_DEF(TConfig, Config);
public:
    using TPtr = std::shared_ptr<TExecutorController>;
    TExecutorController(const NActors::TActorIdentity& executorActorId, const TConfig& config)
        : ExecutorActorId(executorActorId)
        , Config(config)
    {

    }

    TString GetTableName() const {
        return Config.GetTablePath();
    }

    const NInternal::NRequest::TConfig& GetRequestConfig() const {
        return Config.GetRequestConfig();
    }

    void LockPingerFinished() const;
    void TaskFetched(const TTask& task) const;
    void TaskFinished(const TString& taskId) const;
    void AssignFinished() const;
    void FetchingFinished() const;
};

}
