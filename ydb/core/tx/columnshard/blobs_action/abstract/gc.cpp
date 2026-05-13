#include "gc.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS

namespace NKikimr::NOlap {

void IBlobsGCAction::OnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (!AbortedFlag) {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", self.TabletID());
        YDB_LOG_DEBUG("",
            {"event", "OnCompleteTxAfterCleaning"},
            {"action_guid", GetActionGuid()});
        auto storage = self.GetStoragesManager()->GetOperatorVerified(GetStorageId());
        storage->GetSharedBlobs()->OnTransactionCompleteAfterCleaning(BlobsToRemove);
        ui64 sumBytesRemove = 0;
        ui32 blobsCount = 0;
        for (auto i = BlobsToRemove.GetIterator(); i.IsValid(); ++i) {
            Counters->OnReply(i.GetBlobId().BlobSize());
            sumBytesRemove += i.GetBlobId().BlobSize();
            ++blobsCount;
        }
        Counters->OnGCFinished(sumBytesRemove, blobsCount);

        if (!DoOnCompleteTxAfterCleaning(self, taskAction)) {
            return;
        }
        OnFinished();
        NYDBTest::TControllers::GetColumnShardController()->OnAfterGCAction(self, *taskAction);
    }
}

void IBlobsGCAction::OnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) {
    if (!AbortedFlag) {
        const NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("tablet_id", self.TabletID());
        auto storage = self.GetStoragesManager()->GetOperatorVerified(GetStorageId());
        storage->GetSharedBlobs()->OnTransactionExecuteAfterCleaning(BlobsToRemove, dbBlobs.GetDatabase());
        for (auto i = BlobsToRemove.GetIterator(); i.IsValid(); ++i) {
            RemoveBlobIdFromDB(i.GetTabletId(), i.GetBlobId(), dbBlobs);
        }
        YDB_LOG_DEBUG("",
            {"event", "OnExecuteTxAfterCleaning"},
            {"action_guid", GetActionGuid()});
        return DoOnExecuteTxAfterCleaning(self, dbBlobs);
    }
}

void IBlobsGCAction::OnCompleteTxBeforeCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (!AbortedFlag) {
        if (!DoOnCompleteTxBeforeCleaning(self, taskAction)) {
            return;
        }
    }
}

void IBlobsGCAction::OnExecuteTxBeforeCleaning(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs) {
    if (!AbortedFlag) {
        return DoOnExecuteTxBeforeCleaning(self, dbBlobs);
    }
}

void IBlobsGCAction::Abort() {
    Y_ABORT_UNLESS(IsInProgress());
    YDB_LOG_DEBUG("",
        {"event", "gc_aborted"},
        {"action_guid", GetActionGuid()});
    AbortedFlag = true;
}

void IBlobsGCAction::OnFinished() {
    Y_ABORT_UNLESS(IsInProgress());
    FinishedFlag = true;
}

}   // namespace NKikimr::NOlap
