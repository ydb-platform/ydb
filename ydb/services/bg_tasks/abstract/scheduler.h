#pragma once
#include "interface.h"
#include "state.h"

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/events.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NBackgroundTasks {

class ITaskScheduler: public IStringSerializable {
public:
    using TPtr = std::shared_ptr<ITaskScheduler>;
    using TFactory = NObjectFactory::TObjectFactory<ITaskScheduler, TString>;
protected:
    virtual std::optional<TInstant> DoGetNextStartInstant(const TInstant currentStartInstant, const TTaskStateContainer& state) const = 0;
    virtual TInstant DoGetStartInstant() const = 0;
public:
    virtual TString GetClassName() const = 0;
    std::optional<TInstant> GetNextStartInstant(const TInstant currentStartInstant, const TTaskStateContainer& state) const {
        return DoGetNextStartInstant(currentStartInstant, state);
    }
    TInstant GetStartInstant() const {
        return DoGetStartInstant();
    }
};

class TTaskSchedulerContainer: public TInterfaceStringContainer<ITaskScheduler> {
private:
    using TBase = TInterfaceStringContainer<ITaskScheduler>;
public:
    using TBase::TBase;

    std::optional<TInstant> GetNextStartInstant(const TInstant currentStartInstant, const TTaskStateContainer& state) const {
        if (!Object) {
            return TInstant::Zero();
        }
        return Object->GetNextStartInstant(currentStartInstant, state);
    }

    TInstant GetStartInstant() const {
        if (!Object) {
            return TInstant::Zero();
        }
        return Object->GetStartInstant();
    }
};

class IJsonTaskScheduler: public IJsonStringSerializable<ITaskScheduler> {

};

}
