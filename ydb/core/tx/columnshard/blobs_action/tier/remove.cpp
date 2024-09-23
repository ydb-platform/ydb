#include "remove.h"
#include <util/string/join.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TDeclareRemovingAction::DoOnCompleteTxAfterRemoving(const bool blobsWroteSuccessfully) {
    if (blobsWroteSuccessfully) {
        for (auto&& i : GetDeclaredBlobs()) {
            if (GCInfo->IsBlobInUsage(i.first)) {
                AFL_VERIFY(GCInfo->MutableBlobsToDeleteInFuture().Add(i.first, i.second));
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "blob_to_delete")
                    ("blob_id", i.first)("tablet_ids", JoinSeq(",", i.second));
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

}
