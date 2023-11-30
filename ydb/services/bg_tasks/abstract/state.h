#pragma once
#include "interface.h"

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/events.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NBackgroundTasks {
class ITaskState: public IStringSerializable {
public:
    using TPtr = std::shared_ptr<ITaskState>;
    using TFactory = NObjectFactory::TObjectFactory<ITaskState, TString>;
public:
    virtual TString GetClassName() const = 0;
};

class TTaskStateContainer: public TInterfaceStringContainer<ITaskState> {
private:
    using TBase = TInterfaceStringContainer<ITaskState>;
public:
    using TBase::TBase;
};

}
