#include "gc.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

void TGCTask::DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& dbBlobs) {
    size_t numBlobs = 0;

    for (; DraftBlobIds.size() && numBlobs < NColumnShard::TLimits::MAX_BLOBS_TO_DELETE; ++numBlobs) {
        dbBlobs.EraseBlobToKeep(DraftBlobIds.front());
        DraftBlobIds.pop_front();
    }

    for (; DeleteBlobIds.size() && numBlobs < NColumnShard::TLimits::MAX_BLOBS_TO_DELETE; ++numBlobs) {
        dbBlobs.EraseBlobToDelete(DeleteBlobIds.front());
        DeleteBlobIds.pop_front();
    }
}

bool TGCTask::DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (DraftBlobIds.size() || DeleteBlobIds.size()) {
        TActorContext::AsActorContext().Send(self.SelfId(), std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(taskAction));
        return false;
    } else {
        for (auto&& i : DraftBlobIds) {
            Counters->OnReply(i.BlobSize());
        }
        for (auto&& i : DeleteBlobIds) {
            Counters->OnReply(i.BlobSize());
        }
        return true;
    }
}

}
