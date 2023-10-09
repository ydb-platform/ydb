#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class ITask: public NColumnShard::TMonitoringObjectsCounter<ITask> {
private:
    THashMap<TBlobRange, std::shared_ptr<IBlobsReadingAction>> BlobsWaiting;
    std::vector<std::shared_ptr<IBlobsReadingAction>> Agents;
    bool BlobsFetchingStarted = false;
    bool TaskFinishedWithError = false;
    bool DataIsReadyFlag = false;
    const ui64 TaskIdentifier = 0;
    const TString ExternalTaskId;
    bool AbortFlag = false;
    std::optional<ui64> WaitBlobsSize;
    std::optional<ui64> WaitBlobsCount;
    TString TaskCustomer;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    ui32 BlobErrorsCount = 0;
    ui32 BlobsDataCount = 0;
    bool ResultsExtracted = false;
protected:
    bool IsFetchingStarted() const {
        return BlobsFetchingStarted;
    }

    THashMap<TBlobRange, TString> ExtractBlobsData();

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) = 0;
    virtual bool DoOnError(const TBlobRange& range) = 0;

    void OnDataReady();
    bool OnError(const TBlobRange& range);

    virtual TString DoDebugString() const {
        return "";
    }
public:
    void Abort() {
        AbortFlag = true;
    }

    ui64 GetTaskIdentifier() const {
        return TaskIdentifier;
    }

    const TString& GetExternalTaskId() const {
        return ExternalTaskId;
    }

    TString DebugString() const;

    ui64 GetExpectedBlobsSize() const {
        Y_ABORT_UNLESS(WaitBlobsSize);
        return *WaitBlobsSize;
    }

    ui64 GetExpectedBlobsCount() const {
        Y_ABORT_UNLESS(WaitBlobsCount);
        return *WaitBlobsCount;
    }

    THashSet<TBlobRange> GetExpectedRanges() const {
        THashSet<TBlobRange> result;
        for (auto&& i : Agents) {
            i->FillExpectedRanges(result);
        }
        return result;
    }

    const std::vector<std::shared_ptr<IBlobsReadingAction>>& GetAgents() const;

    virtual ~ITask();

    ITask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions, const TString& taskCustomer, const TString& externalTaskId = "");

    void StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress);

    bool AddError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status);
    void AddData(const TBlobRange& range, const TString& data);

    class TReadSubscriber: public NResourceBroker::NSubscribe::ITask {
    private:
        using TBase = NResourceBroker::NSubscribe::ITask;
        const TActorId ReadActorId;
        std::shared_ptr<NRead::ITask> Task;
    protected:
        virtual void DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) override;
    public:
        TReadSubscriber(const TActorId& readActor, const std::shared_ptr<NRead::ITask>& readTask, const ui32 cpu, const ui64 memory, const TString& name,
            const NResourceBroker::NSubscribe::TTaskContext& context)
            : TBase(cpu, memory, name, context)
            , ReadActorId(readActor)
            , Task(readTask)
        {

        }
    };
};

}
