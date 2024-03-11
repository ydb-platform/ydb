#pragma once
#include "blob_set.h"
#include "common.h"
#include <util/generic/hash_set.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_declare.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {
class TBlobManagerDb;

class IBlobsDeclareRemovingAction: public ICommonBlobsAction {
private:
    const TTabletId SelfTabletId;
    using TBase = ICommonBlobsAction;
    std::shared_ptr<NBlobOperations::TRemoveDeclareCounters> Counters;
    YDB_READONLY_DEF(TTabletsByBlob, DeclaredBlobs);
protected:
    virtual void DoDeclareRemove(const TTabletId tabletId, const TUnifiedBlobId& blobId) = 0;
    virtual void DoOnExecuteTxAfterRemoving(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) = 0;
    virtual void DoOnCompleteTxAfterRemoving(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) = 0;
public:
    IBlobsDeclareRemovingAction(const TString& storageId, const TTabletId& selfTabletId, const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters)
        : TBase(storageId)
        , SelfTabletId(selfTabletId)
        , Counters(counters)
    {

    }

    void DeclareRemove(const TTabletId tabletId, const TUnifiedBlobId& blobId);
    void DeclareSelfRemove(const TUnifiedBlobId& blobId);
    void OnExecuteTxAfterRemoving(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
        return DoOnExecuteTxAfterRemoving(self, dbBlobs, blobsWroteSuccessfully);
    }
    void OnCompleteTxAfterRemoving(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) {
        return DoOnCompleteTxAfterRemoving(self, blobsWroteSuccessfully);
    }
};

}
