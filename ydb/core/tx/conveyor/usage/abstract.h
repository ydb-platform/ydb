#pragma once
#include <memory>
#include <ydb/core/tx/columnshard/counters/common/owner.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/string.h>

namespace NKikimr::NConveyor {

class TTaskSignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    NMonitoring::TDynamicCounters::TCounterPtr Fails;
    NMonitoring::TDynamicCounters::TCounterPtr FailsDuration;
    NMonitoring::TDynamicCounters::TCounterPtr Success;
    NMonitoring::TDynamicCounters::TCounterPtr SuccessDuration;

    TTaskSignals(const TString& moduleId, const TString& taskClassIdentifier, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals = nullptr)
        : TBase(moduleId, baseSignals)
    {
        DeepSubGroup("task_class", taskClassIdentifier);
        Fails = TBase::GetDeriviative("Fails");
        FailsDuration = TBase::GetDeriviative("FailsDuration");
        Success = TBase::GetDeriviative("Success");
        SuccessDuration = TBase::GetDeriviative("SuccessDuration");
    }
};

class ITask {
public:
    enum EPriority: ui32 {
        High = 1000,
        Normal = 500,
        Low = 0
    };
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_ACCESSOR(EPriority, Priority, EPriority::Normal);
protected:
    ITask& SetErrorMessage(const TString& message) {
        ErrorMessage = message;
        return *this;
    }
    virtual bool DoExecute() = 0;
public:
    using TPtr = std::shared_ptr<ITask>;
    virtual ~ITask() = default;

    virtual TString GetTaskClassIdentifier() const = 0;

    bool HasError() const {
        return !!ErrorMessage;
    }

    bool Execute(std::shared_ptr<TTaskSignals> signals);
};

}
