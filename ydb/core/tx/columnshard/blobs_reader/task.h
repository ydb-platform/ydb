#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/protos/base.pb.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class ITask {
public:
    using TErrorStatus = TConclusionSpecialStatus<NKikimrProto::EReplyStatus, NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::ERROR>;
private:
    THashMap<TBlobRange, std::shared_ptr<IBlobsReadingAction>> BlobsWaiting;
    std::vector<std::shared_ptr<IBlobsReadingAction>> Agents;
    THashMap<TBlobRange, TString> BlobsData;
    THashMap<TBlobRange, TErrorStatus> BlobErrors;
    bool BlobsFetchingStarted = false;
    bool TaskFinishedWithError = false;
    bool DataIsReadyFlag = false;
    const ui64 TaskIdentifier = 0;
protected:
    bool IsFetchingStarted() const {
        return BlobsFetchingStarted;
    }

    const THashMap<TBlobRange, TString>& GetBlobsData() const {
        return BlobsData;
    }

    THashMap<TBlobRange, TString> ExtractBlobsData() {
        return std::move(BlobsData);
    }

    virtual void DoOnDataReady() = 0;
    virtual bool DoOnError(const TBlobRange& range) = 0;

    void OnDataReady();
    bool OnError(const TBlobRange& range);

    virtual TString DoDebugString() const {
        return "";
    }
public:
    ui64 GetTaskIdentifier() const {
        return TaskIdentifier;
    }

    TString DebugString() const;

    ui64 GetExpectedBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : BlobsWaiting) {
            result += i.second->GetExpectedBlobsSize();
        }
        return result;
    }

    ui64 GetExpectedBlobsCount() const {
        ui64 result = 0;
        for (auto&& i : BlobsWaiting) {
            result += i.second->GetExpectedBlobsCount();
        }
        return result;
    }

    THashSet<TBlobRange> GetExpectedRanges() const {
        THashSet<TBlobRange> result;
        for (auto&& i : BlobsWaiting) {
            i.second->FillExpectedRanges(result);
        }
        return result;
    }

    const std::vector<std::shared_ptr<IBlobsReadingAction>>& GetAgents() const;

    virtual ~ITask() {
        Y_VERIFY(DataIsReadyFlag || TaskFinishedWithError);
    }

    ITask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions);

    void StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress);

    bool AddError(const TBlobRange& range, const TErrorStatus& status);
    void AddData(const TBlobRange& range, const TString& data);
};

}
