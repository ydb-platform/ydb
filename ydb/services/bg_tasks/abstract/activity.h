#pragma once
#include "interface.h"
#include "state.h"

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/events.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NBackgroundTasks {

class ITaskExecutorController {
protected:
    virtual void DoTaskInterrupted(ITaskState::TPtr taskState) = 0;
    virtual void DoTaskFinished() = 0;

public:
    using TPtr = std::shared_ptr<ITaskExecutorController>;
    virtual ~ITaskExecutorController() = default;

    void TaskInterrupted(ITaskState::TPtr taskState) {
        return DoTaskInterrupted(taskState);
    }
    virtual void TaskFinished() {
        return DoTaskFinished();
    }
};

class ITaskActivity: public IStringSerializable {
public:
    using TPtr = std::shared_ptr<ITaskActivity>;
    using TFactory = NObjectFactory::TObjectFactory<ITaskActivity, TString>;
protected:
    virtual void DoExecute(ITaskExecutorController::TPtr controller, const TTaskStateContainer& state) = 0;
    virtual void DoFinished(const TTaskStateContainer& /*state*/) {

    }
public:
    virtual TString GetClassName() const = 0;
    void Execute(ITaskExecutorController::TPtr controller, const TTaskStateContainer& state) {
        DoExecute(controller, state);
    };
    void Finished(const TTaskStateContainer& state) {
        DoFinished(state);
    };
};

class TTaskActivityContainer: public TInterfaceStringContainer<ITaskActivity> {
private:
    using TBase = TInterfaceStringContainer<ITaskActivity>;
public:
    using TBase::TBase;
};

}
