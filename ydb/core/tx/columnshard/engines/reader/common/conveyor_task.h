#pragma once

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader {

class IDataReader;

class IApplyAction {
private:
    bool AppliedFlag = false;

protected:
    virtual bool DoApply(IDataReader& indexedDataRead) = 0;

public:
    bool Apply(IDataReader& indexedDataRead) {
        AFL_VERIFY(!AppliedFlag);
        AppliedFlag = true;
        return DoApply(indexedDataRead);
    }
    virtual ui64 GetSourceId() const {
        return 0;
    }
    virtual ui64 GetBlobBytes() const {
        return 0;
    }
    virtual ui64 GetRawBytes() const {
        return 0;
    }
    virtual ui32 GetFilteredRows() const {
        return 0;
    }
    virtual ui32 GetTotalRows() const {
        return 0;
    }
    virtual ui64 GetTotalReservedBytes() const {
        return 0;
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
        virtual TConclusion<bool> DoExecuteImpl() = 0;

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
