#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/protos/base.pb.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class ITask {
public:
    using TErrorStatus = TConclusionSpecialStatus<NKikimrProto::EReplyStatus, NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::ERROR>;
private:
    YDB_READONLY_DEF(THashSet<TBlobRange>, BlobsWaiting);
    THashMap<TBlobRange, TString> BlobsData;
    THashMap<TBlobRange, TErrorStatus> BlobErrors;
    bool BlobsFetchingStarted = false;
    bool TaskFinishedWithError = false;
    bool DataIsReadyFlag = false;
protected:
    const THashMap<TBlobRange, TString>& GetBlobsData() const {
        return BlobsData;
    }

    THashMap<TBlobRange, TString> ExtractBlobsData() {
        return std::move(BlobsData);
    }

    virtual void DoOnDataReady() = 0;
    virtual bool DoOnError(const TBlobRange& range) = 0;

    void OnDataReady() {
        Y_VERIFY(!DataIsReadyFlag);
        DataIsReadyFlag = true;
        DoOnDataReady();
    }

    bool OnError(const TBlobRange& range) {
        return DoOnError(range);
    }

public:
    THashMap<TUnifiedBlobId, std::vector<TBlobRange>> GetBlobsGroupped() const;

    virtual ~ITask() {
        Y_VERIFY(DataIsReadyFlag || TaskFinishedWithError);
    }

    ITask(const THashSet<TBlobRange>& blobs);

    void StartBlobsFetching();

    bool AddError(const TBlobRange& range, const TErrorStatus& status);
    void AddData(const TBlobRange& range, const TString& data);
};

}
