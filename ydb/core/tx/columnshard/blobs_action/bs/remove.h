#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/remove.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
    NColumnShard::TBlobManager* Manager;
protected:
    virtual void DoDeclareRemove(const TUnifiedBlobId& /*blobId*/) {

    }

    virtual void DoOnExecuteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& dbBlobs, const bool /*success*/) {
        for (auto&& i : GetDeclaredBlobs()) {
            Manager->DeleteBlob(i, dbBlobs);
        }
    }
    virtual void DoOnCompleteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/) {

    }
public:
    TDeclareRemovingAction(const TString& storageId, NColumnShard::TBlobManager& manager)
        : TBase(storageId)
        , Manager(&manager)
    {

    }
};

}
