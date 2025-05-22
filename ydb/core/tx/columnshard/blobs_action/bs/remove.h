#pragma once

#include "blob_manager.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/remove.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
    TBlobManager* Manager;
protected:
    virtual void DoDeclareRemove(const TTabletId /*tabletId*/, const TUnifiedBlobId& /*blobId*/) {

    }

    virtual void DoOnExecuteTxAfterRemoving(TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
        if (blobsWroteSuccessfully) {
            for (auto i = GetDeclaredBlobs().GetIterator(); i.IsValid(); ++i) {
                Manager->DeleteBlobOnExecute(i.GetTabletId(), i.GetBlobId(), dbBlobs);
            }
        }
    }
    virtual void DoOnCompleteTxAfterRemoving(const bool blobsWroteSuccessfully) {
        if (blobsWroteSuccessfully) {
            for (auto i = GetDeclaredBlobs().GetIterator(); i.IsValid(); ++i) {
                Manager->DeleteBlobOnComplete(i.GetTabletId(), i.GetBlobId());
            }
        }
    }
public:
    TDeclareRemovingAction(const TString& storageId, const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters, TBlobManager& manager)
        : TBase(storageId, manager.GetSelfTabletId(), counters)
        , Manager(&manager)
    {

    }
};

}
