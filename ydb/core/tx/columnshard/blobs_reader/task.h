#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TCompositeReadBlobs {
private:
    THashMap<TString, TActionReadBlobs> BlobsByStorage;
public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (auto&& i : BlobsByStorage) {
            sb << "{storage_id:" << i.first << ";blobs:" << i.second.DebugString() << "};";
        }
        sb << "}";
        return sb;
    }

    void Merge(TCompositeReadBlobs&& blobs) {
        for (auto&& i : blobs.BlobsByStorage) {
            BlobsByStorage[i.first].Merge(std::move(i.second));
        }
    }

    void Clear() {
        BlobsByStorage.clear();
    }

    bool IsEmpty() const {
        return BlobsByStorage.empty();
    }

    THashMap<TString, TActionReadBlobs>::iterator begin() {
        return BlobsByStorage.begin();
    }
    THashMap<TString, TActionReadBlobs>::iterator end() {
        return BlobsByStorage.end();
    }
    void Add(const TString& storageId, TActionReadBlobs&& data) {
        AFL_VERIFY(BlobsByStorage.emplace(storageId, std::move(data)).second);
    }
    void Add(const TString& storageId, const TBlobRange& blobId, TString&& value) {
        BlobsByStorage[storageId].Add(blobId, std::move(value));
    }
    bool Contains(const TString& storageId, const TBlobRange& range) const {
        auto it = BlobsByStorage.find(storageId);
        if (it == BlobsByStorage.end()) {
            return false;
        }
        return it->second.Contains(range);
    }
    std::optional<TString> GetBlobRangeOptional(const TString& storageId, const TBlobRange& range) const {
        auto it = BlobsByStorage.find(storageId);
        if (it == BlobsByStorage.end()) {
            return {};
        }
        return it->second.GetBlobRangeOptional(range);
    }
    TString Extract(const TString& storageId, const TBlobRange& range) {
        auto it = BlobsByStorage.find(storageId);
        AFL_VERIFY(it != BlobsByStorage.end());
        auto result = it->second.Extract(range);
        if (it->second.IsEmpty()) {
            BlobsByStorage.erase(it);
        }
        return result;
    }

    ui64 GetTotalBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : BlobsByStorage) {
            result += i.second.GetTotalBlobsSize();
        }
        return result;
    }
};

class ITask: public NColumnShard::TMonitoringObjectsCounter<ITask> {
private:
    THashMap<TString, std::shared_ptr<IBlobsReadingAction>> AgentsWaiting;
    YDB_READONLY_DEF(TReadActionsCollection, Agents);
    bool BlobsFetchingStarted = false;
    bool TaskFinishedWithError = false;
    bool DataIsReadyFlag = false;
    const ui64 TaskIdentifier = 0;
    const TString ExternalTaskId;
    bool AbortFlag = false;
    TString TaskCustomer;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    i64 BlobsWaitingCount = 0;
    bool ResultsExtracted = false;
protected:
    bool IsFetchingStarted() const {
        return BlobsFetchingStarted;
    }

    TCompositeReadBlobs ExtractBlobsData();

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) = 0;
    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) = 0;

    void OnDataReady();
    bool OnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status);

    virtual TString DoDebugString() const {
        return "";
    }
public:
    i64 GetWaitingRangesCount() const {
        return BlobsWaitingCount;
    }

    void Abort() {
        AbortFlag = true;
    }

    bool IsFinished() const {
        return AgentsWaiting.empty() && BlobsFetchingStarted;
    }

    ui64 GetTaskIdentifier() const {
        return TaskIdentifier;
    }

    const TString& GetExternalTaskId() const {
        return ExternalTaskId;
    }

    TString DebugString() const;

    virtual ~ITask();

    ITask(const TReadActionsCollection& actions, const TString& taskCustomer, const TString& externalTaskId = "");

    void StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress);

    bool AddError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status);
    void AddData(const TString& storageId, const TBlobRange& range, const TString& data);

    class TReadSubscriber: public NResourceBroker::NSubscribe::ITask {
    private:
        using TBase = NResourceBroker::NSubscribe::ITask;
        std::shared_ptr<NRead::ITask> Task;
    protected:
        virtual void DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) override;
    public:
        TReadSubscriber(const std::shared_ptr<NRead::ITask>& readTask, const ui32 cpu, const ui64 memory, const TString& name,
            const NResourceBroker::NSubscribe::TTaskContext& context)
            : TBase(cpu, memory, name, context)
            , Task(readTask)
        {

        }
    };
};

}
