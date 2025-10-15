#pragma once
#include <memory>
#include <ydb/library/signals/owner.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/conclusion/status.h>

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

    TTaskSignals(const NColumnShard::TCommonCountersOwner& baseObject, const TString& taskClassIdentifier)
        : TBase(baseObject, "task_class_name", taskClassIdentifier) {
        Fails = TBase::GetDeriviative("Fails");
        FailsDuration = TBase::GetDeriviative("FailsDuration");
        Success = TBase::GetDeriviative("Success");
        SuccessDuration = TBase::GetDeriviative("SuccessDuration");
    }

    TTaskSignals(const TString& moduleId, const TString& taskClassIdentifier, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals = nullptr)
        : TBase(moduleId, baseSignals) {
        DeepSubGroup("task_class", taskClassIdentifier);
        Fails = TBase::GetDeriviative("Fails");
        FailsDuration = TBase::GetDeriviative("FailsDuration");
        Success = TBase::GetDeriviative("Success");
        SuccessDuration = TBase::GetDeriviative("SuccessDuration");
    }
};

class TProcessGuard: TNonCopyable {
private:
    const ui64 ProcessId;
    bool Finished = false;
    const std::optional<NActors::TActorId> ServiceActorId;
public:
    ui64 GetProcessId() const {
        return ProcessId;
    }

    explicit TProcessGuard(const ui64 processId, const std::optional<NActors::TActorId>& actorId)
        : ProcessId(processId)
        , ServiceActorId(actorId) {

    }

    void Finish();

    ~TProcessGuard() {
        if (!Finished) {
            Finish();
        }
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
    YDB_ACCESSOR(EPriority, Priority, EPriority::Normal);
    bool ExecutedFlag = false;
protected:
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) = 0;
    virtual void DoOnCannotExecute(const TString& reason);
public:
    using TPtr = std::shared_ptr<ITask>;
    virtual ~ITask() = default;

    virtual TString GetTaskClassIdentifier() const = 0;

    void OnCannotExecute(const TString& reason) {
        return DoOnCannotExecute(reason);
    }
    void Execute(std::shared_ptr<TTaskSignals> signals, const std::shared_ptr<ITask>& taskPtr);
};

}
