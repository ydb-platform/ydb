#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_gc.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGCTask: public IBlobsGCAction {
private:
    using TBase = IBlobsGCAction;
private:
    YDB_READONLY_DEF(std::deque<TUnifiedBlobId>, DraftBlobIds);
    YDB_READONLY_DEF(std::deque<TUnifiedBlobId>, DeleteBlobIds);
    YDB_READONLY_DEF(NWrappers::NExternalStorage::IExternalStorageOperator::TPtr, ExternalStorageOperator);
    const std::shared_ptr<TRemoveGCCounters> Counters;
protected:
    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) override;
    virtual bool DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) override;
public:
    TGCTask(const TString& storageId, std::deque<TUnifiedBlobId>&& draftBlobIds, std::deque<TUnifiedBlobId>&& deleteBlobIds,
        const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& externalStorageOperator, const std::shared_ptr<TRemoveGCCounters>& counters)
        : TBase(storageId)
        , DraftBlobIds(std::move(draftBlobIds))
        , DeleteBlobIds(std::move(deleteBlobIds))
        , ExternalStorageOperator(externalStorageOperator)
        , Counters(counters)
    {
        for (auto&& i : DraftBlobIds) {
            Counters->OnRequest(i.BlobSize());
        }
        for (auto&& i : DeleteBlobIds) {
            Counters->OnRequest(i.BlobSize());
        }
    }
};

}
