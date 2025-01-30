#pragma once
#include "common.h"
#include <util/generic/hash.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/write.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {

class IBlobsWritingAction: public ICommonBlobsAction {
private:
    using TBase = ICommonBlobsAction;
    bool WritingStarted = false;
    THashMap<TUnifiedBlobId, TMonotonic> WritingStart;
    ui64 SumSize = 0;
    ui32 BlobsWriteCount = 0;
    THashMap<TUnifiedBlobId, TString> BlobsForWrite;
    THashSet<TUnifiedBlobId> BlobsWaiting;
    bool Aborted = false;
    std::shared_ptr<NBlobOperations::TWriteCounters> Counters;
    void AddDataForWrite(const TUnifiedBlobId& blobId, const TString& data);
protected:
    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) = 0;
    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) = 0;

    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) = 0;
    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) = 0;

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) = 0;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) = 0;

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) = 0;
public:
    IBlobsWritingAction(const TString& storageId)
        : TBase(storageId)
    {

    }
    virtual ~IBlobsWritingAction();
    bool IsReady() const;

    void Merge(const std::shared_ptr<IBlobsWritingAction>& action) {
        AFL_VERIFY(action);
        AFL_VERIFY(!WritingStarted);
        for (auto&& i : action->BlobsForWrite) {
            AddDataForWrite(i.first, i.second);
        }
    }

    void SetCounters(std::shared_ptr<NBlobOperations::TWriteCounters> counters) {
        Counters = counters;
    }

    const THashMap<TUnifiedBlobId, TString>& GetBlobsForWrite() const {
        return BlobsForWrite;
    }

    void Abort() {
        Aborted = true;
    }
    TUnifiedBlobId AddDataForWrite(const TString& data, const std::optional<TUnifiedBlobId>& externalBlobId = {});
    void OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status);

    void OnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) {
        return DoOnExecuteTxBeforeWrite(self, dbBlobs);
    }

    ui32 GetBlobsCount() const {
        return BlobsWriteCount;
    }
    ui32 GetTotalSize() const {
        return SumSize;
    }

    virtual bool NeedDraftTransaction() const = 0;

    void OnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) {
        return DoOnCompleteTxBeforeWrite(self);
    }

    void OnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
        return DoOnExecuteTxAfterWrite(self, dbBlobs, blobsWroteSuccessfully);
    }

    void OnCompleteTxAfterWrite(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) {
        return DoOnCompleteTxAfterWrite(self, blobsWroteSuccessfully);
    }

    void SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId);
};

class TWriteActionsCollection {
private:
    THashMap<TString, std::shared_ptr<IBlobsWritingAction>> Actions;
public:
    THashMap<TString, std::shared_ptr<IBlobsWritingAction>>::const_iterator begin() const {
        return Actions.begin();
    }

    THashMap<TString, std::shared_ptr<IBlobsWritingAction>>::const_iterator end() const {
        return Actions.end();
    }

    THashMap<TString, std::shared_ptr<IBlobsWritingAction>>::iterator begin() {
        return Actions.begin();
    }

    THashMap<TString, std::shared_ptr<IBlobsWritingAction>>::iterator end() {
        return Actions.end();
    }

    std::shared_ptr<IBlobsWritingAction> Add(const std::shared_ptr<IBlobsWritingAction>& action) {
        auto it = Actions.find(action->GetStorageId());
        if (it == Actions.end()) {
            return Actions.emplace(action->GetStorageId(), action).first->second;
        } else if (action.get() != it->second.get()) {
            it->second->Merge(action);
        }
        return it->second;
    }

    TWriteActionsCollection() = default;

    TWriteActionsCollection(const std::vector<std::shared_ptr<IBlobsWritingAction>>& actions) {
        for (auto&& a : actions) {
            Add(a);
        }
    }
};

}
