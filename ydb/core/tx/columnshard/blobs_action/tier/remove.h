#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/remove.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include "gc_info.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
    std::shared_ptr<TGCInfo> GCInfo;
protected:
    virtual void DoDeclareRemove(const TTabletId /*tabletId*/, const TUnifiedBlobId& /*blobId*/) {

    }

    virtual void DoOnExecuteTxAfterRemoving(TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully);
    virtual void DoOnCompleteTxAfterRemoving(const bool blobsWroteSuccessfully);
public:
    TDeclareRemovingAction(const TString& storageId, const TTabletId selfTabletId, const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters, const std::shared_ptr<TGCInfo>& gcInfo)
        : TBase(storageId, selfTabletId, counters)
        , GCInfo(gcInfo)
    {

    }
};

}
