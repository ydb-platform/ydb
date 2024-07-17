#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>

namespace NKikimr::NOlap::NBlobOperations::NLocal {

class TOperator: public IBlobsStorageOperator {
private:
    using TBase = IBlobsStorageOperator;
    NSplitter::TSplitSettings SplitSettings = Default<NSplitter::TSplitSettings>();

protected:
    virtual const NSplitter::TSplitSettings& DoGetBlobSplitSettings() const override {
        return SplitSettings;
    }
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction(
        const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& /*counters*/) override;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override {
        AFL_VERIFY(false)("problem", "unimplemented method");
        return nullptr;
    };
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override {
        AFL_VERIFY(false)("problem", "unimplemented method");
        return nullptr;
    };
    virtual std::shared_ptr<IBlobsGCAction> DoCreateGCAction(const std::shared_ptr<TRemoveGCCounters>& /*counters*/) const override {
        return nullptr;
    }
    virtual void DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& /*action*/) const override {
        AFL_VERIFY(false)("problem", "unimplemented method");
    };
    virtual bool DoLoad(IBlobManagerDb& /*dbBlobs*/) override {
        return true;
    };
    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& /*tiers*/) override {
        return;
    };

public:
    TOperator(const TString& storageId, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager);

    virtual TTabletsByBlob GetBlobsToDelete() const override {
        return Default<TTabletsByBlob>();
    }

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const override;

    virtual bool HasToDelete(const TUnifiedBlobId& /*blobId*/, const TTabletId /*tabletId*/) const override {
        return false;
    }

};

}
