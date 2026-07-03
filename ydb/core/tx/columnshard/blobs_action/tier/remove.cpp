#include "remove.h"

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TDeclareRemovingAction::DoOnCompleteTxAfterRemoving(const bool blobsWroteSuccessfully) {
    if (blobsWroteSuccessfully) {
        for (auto&& i : GetDeclaredBlobs()) {
            if (GCInfo->IsBlobInUsage(i.first)) {
                AFL_VERIFY(GCInfo->MutableBlobsToDeleteInFuture().Add(i.first, i.second));
            } else {
                YDB_LOG_DEBUG("Dump event, blobId, tabletIds",
                    {"event", "blob_to_delete"},
                    {"blobId", i.first},
                    {"tabletIds", JoinSeq(",", i.second)});
                AFL_VERIFY(GCInfo->MutableBlobsToDelete().Add(i.first, i.second));
            }
        }
    }
}

void TDeclareRemovingAction::DoOnExecuteTxAfterRemoving(TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
    if (blobsWroteSuccessfully) {
        for (auto i = GetDeclaredBlobs().GetIterator(); i.IsValid(); ++i) {
            dbBlobs.AddTierBlobToDelete(GetStorageId(), i.GetBlobId(), i.GetTabletId());
        }
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
