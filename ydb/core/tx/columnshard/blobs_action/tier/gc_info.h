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
    bool HasToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) const {
        return BlobsToDelete.Contains(tabletId, blobId) || BlobsToDeleteInFuture.Contains(tabletId, blobId);
    }

    virtual void OnBlobFree(const TUnifiedBlobId& blobId) override {
        BlobsToDeleteInFuture.ExtractBlobTo(blobId, BlobsToDelete);
    }

    bool ExtractForGC(std::deque<TUnifiedBlobId>& deleteDraftBlobIds, TTabletsByBlob& deleteBlobIds, const ui32 blobsCountLimit) {
        if (DraftBlobIdsToRemove.empty() && BlobsToDelete.IsEmpty()) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "extract_for_gc_skip")("reason", "no_data");
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "extract_blobs_to_gc")("blob_ids", deleteBlobIdsLocal.DebugString());
        std::swap(deleteBlobIdsLocal, deleteBlobIds);
        return true;
    }
};

}
