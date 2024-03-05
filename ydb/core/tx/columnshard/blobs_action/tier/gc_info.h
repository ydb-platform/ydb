#pragma once
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGCInfo: public TCommonBlobsTracker {
private:
    YDB_ACCESSOR_DEF(TTabletsByBlob, BlobsToDelete);
    YDB_ACCESSOR_DEF(std::deque<TUnifiedBlobId>, DraftBlobIdsToRemove);
    YDB_ACCESSOR_DEF(TTabletsByBlob, BlobsToDeleteInFuture);
public:
    virtual void OnBlobFree(const TUnifiedBlobId& blobId) override {
        BlobsToDeleteInFuture.ExtractBlobTo(blobId, BlobsToDelete);
    }

    bool ExtractForGC(std::deque<TUnifiedBlobId>& deleteDraftBlobIds, TTabletsByBlob& deleteBlobIds, const ui32 blobsCountLimit) {
        if (DraftBlobIdsToRemove.empty() && BlobsToDelete.IsEmpty()) {
            return false;
        }
        ui32 count = 0;
        TTabletsByBlob deleteBlobIdsLocal;
        while (DraftBlobIdsToRemove.size() && count < blobsCountLimit) {
            deleteDraftBlobIds.emplace_back(DraftBlobIdsToRemove.front());
            DraftBlobIdsToRemove.pop_front();
            ++count;
        }
        while (BlobsToDelete.ExtractFrontTo(deleteBlobIdsLocal) && count < blobsCountLimit) {
            ++count;
        }
        std::swap(deleteBlobIdsLocal, deleteBlobIds);
        return true;
    }
};

}
