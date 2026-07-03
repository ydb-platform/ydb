#include "storage.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/size_literals.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS

namespace NKikimr::NOlap {

bool TCommonBlobsTracker::IsBlobInUsage(const NOlap::TUnifiedBlobId& blobId) const {
    return BlobsUseCount.contains(blobId);
}

bool TCommonBlobsTracker::DoUseBlob(const TUnifiedBlobId& blobId) {
    auto it = BlobsUseCount.find(blobId);
    if (it == BlobsUseCount.end()) {
        BlobsUseCount.emplace(blobId, 1);
        YDB_LOG_TRACE("Dump method, blobId, count",
            {"method", "DoUseBlob"},
            {"blobId", blobId},
            {"count", 1});
        return true;
    } else {
        ++it->second;
        YDB_LOG_TRACE("Dump method, blobId, count",
            {"method", "DoUseBlob"},
            {"blobId", blobId},
            {"count", it->second});
        return false;
    }
}

bool TCommonBlobsTracker::DoFreeBlob(const TUnifiedBlobId& blobId) {
    auto useIt = BlobsUseCount.find(blobId);
    AFL_VERIFY(useIt != BlobsUseCount.end())("reason", "Unknown blob")("blob_id", blobId.ToStringNew());
    AFL_VERIFY(useIt->second);
    --useIt->second;
    YDB_LOG_TRACE("Dump method, blobId, count",
        {"method", "DoFreeBlob"},
        {"blobId", blobId},
        {"count", useIt->second});

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

const NSplitter::TSplitSettings& IBlobsStorageOperator::GetBlobSplitSettings() const {
    return NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings(DoGetBlobSplitSettings());
}

ui64 IBlobsStorageOperator::GetSmallBlobThresholdBytes() const {
    const ui64 base = HasAppData() ? AppData()->SmallBlobsQuotaConfig.GetSmallBlobSizeThresholdBytes() : (ui64)64_KB;
    const auto layout = GetBlobStorageLayout();
    return base * (layout ? std::max<ui32>(1, layout->DataParts()) : 1);
}

}   // namespace NKikimr::NOlap
