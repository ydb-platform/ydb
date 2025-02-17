#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_gc.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/common.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGCTask: public IBlobsGCAction {
private:
    using TBase = IBlobsGCAction;
private:
    YDB_READONLY_DEF(std::deque<TUnifiedBlobId>, DraftBlobIds);
    std::shared_ptr<TExternalStorageOperatorHolder> ExternalStorageOperator;
protected:
    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) override;
    virtual bool DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) override;
    virtual void DoOnExecuteTxBeforeCleaning(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& /*dbBlobs*/) override {

    }
    virtual bool DoOnCompleteTxBeforeCleaning(NColumnShard::TColumnShard& /*self*/, const std::shared_ptr<IBlobsGCAction>& /*taskAction*/) override {
        return true;
    }
    virtual void RemoveBlobIdFromDB(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobManagerDb& dbBlobs) override;
    virtual bool DoIsEmpty() const override {
        return DraftBlobIds.empty();
    }
public:
    TGCTask(const TString& storageId, std::deque<TUnifiedBlobId>&& draftBlobIds, const std::shared_ptr<TExternalStorageOperatorHolder>& externalStorageOperator,
        TBlobsCategories&& blobsToRemove, const std::shared_ptr<TRemoveGCCounters>& counters)
        : TBase(storageId, std::move(blobsToRemove), counters)
        , DraftBlobIds(std::move(draftBlobIds))
        , ExternalStorageOperator(externalStorageOperator)
    {
        for (auto&& i : DraftBlobIds) {
            Counters->OnRequest(i.BlobSize());
        }
    }

    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetExternalStorageOperator() const {
        return ExternalStorageOperator->Get();
    }
};

}
