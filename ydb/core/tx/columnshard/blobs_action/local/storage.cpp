#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations::NLocal {

TOperator::TOperator(const TString& storageId, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager)
    : TBase(storageId, storageSharedBlobsManager)
{
}

namespace {
class TBlobInUseTracker: public IBlobInUseTracker {
private:
    virtual bool DoFreeBlob(const NOlap::TUnifiedBlobId& /*blobId*/) override {
        AFL_VERIFY(false);
        return true;
    }
    virtual bool DoUseBlob(const NOlap::TUnifiedBlobId& /*blobId*/) override {
        AFL_VERIFY(false);
        return true;
    }
    virtual bool IsBlobInUsage(const NOlap::TUnifiedBlobId& /*blobId*/) const override {
        AFL_VERIFY(false);
        return false;
    }

public:
};
}

std::shared_ptr<IBlobInUseTracker> TOperator::GetBlobsTracker() const {
    static std::shared_ptr<IBlobInUseTracker> result = std::make_shared<TBlobInUseTracker>();
    return result;
}

namespace {
class TBlobsDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
protected:
    virtual void DoDeclareRemove(const TTabletId /*tabletId*/, const TUnifiedBlobId& /*blobId*/) override {
        AFL_VERIFY(false);
    }
    virtual void DoOnExecuteTxAfterRemoving(TBlobManagerDb& /*dbBlobs*/, const bool /*blobsWroteSuccessfully*/) override {
    }
    virtual void DoOnCompleteTxAfterRemoving(const bool /*blobsWroteSuccessfully*/) override {
    
    }

public:
    TBlobsDeclareRemovingAction(const TString& storageId, const TTabletId selfTabletId, const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters)
        : TBase(storageId, selfTabletId, counters) {
    }
};
}

std::shared_ptr<IBlobsDeclareRemovingAction> TOperator::DoStartDeclareRemovingAction(
    const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) {
    static std::shared_ptr<IBlobsDeclareRemovingAction> result = std::make_shared<TBlobsDeclareRemovingAction>(GetStorageId(), GetSelfTabletId(), counters);
    return result;
}

}
