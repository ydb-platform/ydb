#pragma once
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGCInfo: public TCommonBlobsTracker {
private:
    YDB_ACCESSOR_DEF(std::deque<TUnifiedBlobId>, BlobsToDelete);
    YDB_ACCESSOR_DEF(std::deque<TUnifiedBlobId>, DraftBlobIdsToRemove);
    YDB_ACCESSOR_DEF(THashSet<TUnifiedBlobId>, BlobsToDeleteInFuture);
public:
    virtual void OnBlobFree(const TUnifiedBlobId& blobId) override {
        if (BlobsToDeleteInFuture.erase(blobId)) {
            BlobsToDelete.emplace_back(blobId);
        }
    }

    bool ExtractForGC(std::vector<TUnifiedBlobId>& deleteDraftBlobIds, std::vector<TUnifiedBlobId>& deleteBlobIds, const ui32 blobsCountLimit) {
        if (DraftBlobIdsToRemove.empty() && BlobsToDelete.empty()) {
            return false;
        }
        while (DraftBlobIdsToRemove.size() && deleteBlobIds.size() + deleteDraftBlobIds.size() < blobsCountLimit) {
            deleteDraftBlobIds.emplace_back(DraftBlobIdsToRemove.front());
            DraftBlobIdsToRemove.pop_front();
        }
        while (BlobsToDelete.size() && deleteBlobIds.size() + deleteDraftBlobIds.size() < blobsCountLimit) {
            deleteBlobIds.emplace_back(BlobsToDelete.front());
            BlobsToDelete.pop_front();
        }
        return true;
    }
};

}
