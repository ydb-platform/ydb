#include "gc.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TGCTask::RemoveBlobIdFromDB(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobManagerDb& dbBlobs) {
    dbBlobs.RemoveTierBlobToDelete(GetStorageId(), blobId, tabletId);
}

void TGCTask::DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& dbBlobs) {
    for (auto&& i : DraftBlobIds) {
        dbBlobs.RemoveTierDraftBlobId(GetStorageId(), i);
    }
}

bool TGCTask::DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, const std::shared_ptr<IBlobsGCAction>& /*taskAction*/) {
    return true;
}

}
