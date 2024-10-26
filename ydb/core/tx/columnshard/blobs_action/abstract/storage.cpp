#include "storage.h"

namespace NKikimr::NOlap {

bool TCommonBlobsTracker::IsBlobInUsage(const NOlap::TUnifiedBlobId& blobId) const {
    return BlobsUseCount.contains(blobId);
}

bool TCommonBlobsTracker::DoUseBlob(const TUnifiedBlobId& blobId) {
    const auto& [it, isNew] = BlobsUseCount.emplace(blobId, 1);
    if (!isNew) {
        ++it->second;
    }
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_BLOBS)("method", "DoUseBlob")("blob_id", blobId)("count", it->second);
    return isNew;
}

bool TCommonBlobsTracker::DoFreeBlob(const TUnifiedBlobId& blobId) {
    auto useIt = BlobsUseCount.find(blobId);
    AFL_VERIFY(useIt != BlobsUseCount.end())("reason", "Unknown blob")("blob_id", blobId.ToStringNew());
    AFL_VERIFY(useIt->second);
    --useIt->second;
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_BLOBS)("method", "DoFreeBlob")("blob_id", blobId)("count", useIt->second);

    if (useIt->second > 0) {
        return false;
    }
    BlobsUseCount.erase(useIt);
    OnBlobFree(blobId);
    return true;
}

void IBlobsStorageOperator::Stop() {
    if (CurrentGCAction && CurrentGCAction->IsInProgress()) {
        CurrentGCAction->Abort();
    }
    AFL_VERIFY(DoStop());
    Stopped = true;
}

}
