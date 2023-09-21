#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGCTask: public IBlobsGCAction {
private:
    using TBase = IBlobsGCAction;
private:
    YDB_READONLY_DEF(std::vector<TUnifiedBlobId>, DraftBlobIds);
    YDB_READONLY_DEF(std::vector<TUnifiedBlobId>, DeleteBlobIds);
    YDB_READONLY_DEF(NWrappers::NExternalStorage::IExternalStorageOperator::TPtr, ExternalStorageOperator);
protected:
    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) override;
    virtual void DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, const std::shared_ptr<IBlobsGCAction>& /*taskAction*/) override {

    }
public:
    TGCTask(const TString& storageId, std::vector<TUnifiedBlobId>&& draftBlobIds, std::vector<TUnifiedBlobId>&& deleteBlobIds,
        const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& externalStorageOperator)
        : TBase(storageId)
        , DraftBlobIds(std::move(draftBlobIds))
        , DeleteBlobIds(std::move(deleteBlobIds))
        , ExternalStorageOperator(externalStorageOperator)
    {
    }
};

}
