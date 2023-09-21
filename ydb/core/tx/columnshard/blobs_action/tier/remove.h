#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/remove.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
    std::shared_ptr<TGCInfo> GCInfo;
protected:
    virtual void DoDeclareRemove(const TUnifiedBlobId& /*blobId*/) {

    }

    virtual void DoOnExecuteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) {
        if (success) {
            for (auto&& i : GetDeclaredBlobs()) {
                dbBlobs.AddTierBlobToDelete(GetStorageId(), i);
                if (GCInfo->IsBlobInUsage(i)) {
                    Y_VERIFY(GCInfo->MutableBlobsToDeleteInFuture().emplace(i).second);
                } else {
                    GCInfo->MutableBlobsToDelete().emplace_back(i);
                }
            }
        }
    }
    virtual void DoOnCompleteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/) {

    }
public:
    TDeclareRemovingAction(const TString& storageId, const std::shared_ptr<TGCInfo>& gcInfo)
        : TBase(storageId)
        , GCInfo(gcInfo)
    {

    }
};

}
