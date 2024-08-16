#pragma once
#include "common.h"
#include <util/generic/string.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_gc.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {
class TBlobManagerDb;

class IBlobsGCAction: public ICommonBlobsAction {
private:
    using TBase = ICommonBlobsAction;
protected:
    TBlobsCategories BlobsToRemove;
    std::shared_ptr<NBlobOperations::TRemoveGCCounters> Counters;
protected:
    bool AbortedFlag = false;
    bool FinishedFlag = false;

    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) = 0;
    virtual bool DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) = 0;

    virtual void DoOnExecuteTxBeforeCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) = 0;
    virtual bool DoOnCompleteTxBeforeCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) = 0;

    virtual void RemoveBlobIdFromDB(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobManagerDb& dbBlobs) = 0;
    virtual bool DoIsEmpty() const = 0;
public:
    void AddSharedBlobToNextIteration(const TUnifiedBlobId& blobId, const TTabletId ownerTabletId) {
        AFL_VERIFY(BlobsToRemove.RemoveBorrowed(ownerTabletId, blobId));
    }

    void OnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs);
    void OnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction);

    void OnExecuteTxBeforeCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs);
    void OnCompleteTxBeforeCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction);

    const TBlobsCategories& GetBlobsToRemove() const {
        return BlobsToRemove;
    }

    bool IsEmpty() const {
        return BlobsToRemove.IsEmpty() && DoIsEmpty();
    }

    bool IsInProgress() const {
        return !AbortedFlag && !FinishedFlag;
    }

    IBlobsGCAction(const TString& storageId, TBlobsCategories&& blobsToRemove, const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters)
        : TBase(storageId)
        , BlobsToRemove(std::move(blobsToRemove))
        , Counters(counters)
    {
        for (auto i = BlobsToRemove.GetIterator(); i.IsValid(); ++i) {
            Counters->OnRequest(i.GetBlobId().BlobSize());
        }
    }

    void Abort();
    void OnFinished();
};

}
