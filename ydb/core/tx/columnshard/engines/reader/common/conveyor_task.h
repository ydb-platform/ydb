#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader {

class IDataReader;

class IDataTasksProcessor {
public:
    class ITask: public NConveyor::ITask {
    private:
        using TBase = NConveyor::ITask;
    protected:
        virtual bool DoApply(IDataReader& indexedDataRead) const = 0;
    public:
        ITask(const std::optional<NActors::TActorId> ownerId = {})
            : TBase(ownerId) {

        }

        using TPtr = std::shared_ptr<ITask>;
        virtual ~ITask() = default;
        bool Apply(IDataReader& indexedDataRead) const;
    };
};

}
