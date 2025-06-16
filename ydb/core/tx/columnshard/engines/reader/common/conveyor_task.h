#pragma once

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader {

class IDataReader;

class IApplyAction {
protected:
    virtual bool DoApply(IDataReader& indexedDataRead) const = 0;

public:
    bool Apply(IDataReader& indexedDataRead) const {
        return DoApply(indexedDataRead);
    }
    virtual ~IApplyAction() = default;
};

class IDataTasksProcessor {
public:
    class ITask: public NConveyor::ITask, public IApplyAction {
    private:
        using TBase = NConveyor::ITask;
        const NActors::TActorId OwnerId;
        NColumnShard::TCounterGuard Guard;
        virtual TConclusionStatus DoExecuteImpl() = 0;

    protected:
        virtual void DoExecute(const std::shared_ptr<NConveyor::ITask>& taskPtr) override final;
        virtual void DoOnCannotExecute(const TString& reason) override;

    public:
        using TPtr = std::shared_ptr<ITask>;
        virtual ~ITask() = default;

        ITask(const NActors::TActorId& ownerId, NColumnShard::TCounterGuard&& scanCounter)
            : OwnerId(ownerId)
            , Guard(std::move(scanCounter)) {
        }
    };
};

}   // namespace NKikimr::NOlap::NReader
