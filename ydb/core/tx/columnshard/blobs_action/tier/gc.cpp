#include "gc.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TGCTask::DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& dbBlobs) {
    for (auto&& i : DraftBlobIds) {
        dbBlobs.RemoveTierDraftBlobId(GetStorageId(), i);
    }
    for (auto&& i : DeleteBlobIds) {
        dbBlobs.RemoveTierBlobToDelete(GetStorageId(), i);
    }
}

}
