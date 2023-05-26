#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NIndexedReader {
class TGranulesFillingContext;
}

namespace NKikimr::NColumnShard {

class TDataTasksProcessorContainer;

class IDataTasksProcessor {
private:
    TAtomicCounter DataProcessorAddDataCounter = 0;
    void ReplyReceived() {
        Y_VERIFY(DataProcessorAddDataCounter.Dec() >= 0);
    }
public:
    class ITask: public NConveyor::ITask {
    private:
        std::shared_ptr<IDataTasksProcessor> OwnerOperator;
        bool DataProcessed = false;
    protected:
        TDataTasksProcessorContainer GetTasksProcessorContainer() const;
        virtual bool DoApply(NOlap::NIndexedReader::TGranulesFillingContext& indexedDataRead) const = 0;
        virtual bool DoExecuteImpl() = 0;

        virtual bool DoExecute() override final;
    public:
        ITask(std::shared_ptr<IDataTasksProcessor> ownerOperator)
            : OwnerOperator(ownerOperator) {

        }

        bool IsSameProcessor(const TDataTasksProcessorContainer& receivedProcessor) const;

        using TPtr = std::shared_ptr<ITask>;
        virtual ~ITask() = default;
        bool Apply(NOlap::NIndexedReader::TGranulesFillingContext& indexedDataRead) const;

        bool IsDataProcessed() const noexcept {
            return DataProcessed;
        }
    };
protected:
    virtual bool DoAdd(ITask::TPtr task) = 0;
    std::atomic<bool> Stopped = false;
public:
    i64 GetDataCounter() const {
        return DataProcessorAddDataCounter.Val();
    }

    void Stop() {
        Stopped = true;
    }
    bool IsStopped() const {
        return Stopped;
    }
    bool InWaiting() const {
        return !IsStopped() && DataProcessorAddDataCounter.Val();
    }

    using TPtr = std::shared_ptr<IDataTasksProcessor>;
    virtual ~IDataTasksProcessor() = default;
    bool Add(ITask::TPtr task);
};

class TDataTasksProcessorContainer {
private:
    IDataTasksProcessor::TPtr Object;
public:
    TDataTasksProcessorContainer() = default;
    TDataTasksProcessorContainer(IDataTasksProcessor::TPtr object)
        : Object(object)
    {

    }

    bool IsSameProcessor(const TDataTasksProcessorContainer& container) const {
        return (ui64)Object.get() == (ui64)container.Object.get();
    }

    void Stop() {
        if (Object) {
            Object->Stop();
        }
    }

    bool InWaiting() const {
        return Object && Object->InWaiting();
    }

    bool IsStopped() const {
        return Object && Object->IsStopped();
    }

    IDataTasksProcessor::TPtr GetObject() const noexcept {
        return Object;
    }

    void Add(NOlap::NIndexedReader::TGranulesFillingContext& context, IDataTasksProcessor::ITask::TPtr task);
};

}
