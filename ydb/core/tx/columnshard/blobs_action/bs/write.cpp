#include "write.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TWriteAction::DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) {
    AFL_VERIFY(!!BlobBatch);
    if (blobsWroteSuccessfully) {
        Manager->SaveBlobBatchOnExecute(BlobBatch, dbBlobs);
    }
}

void TWriteAction::DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& self, const bool blobsWroteSuccessfully) {
    AFL_VERIFY(!!BlobBatch);
    ui64 blobsWritten = BlobBatch.GetBlobCount();
    ui64 bytesWritten = BlobBatch.GetTotalSize();
    if (blobsWroteSuccessfully) {
        if (IsBulk()) {
            self.Counters.GetTabletCounters()->OnBulkWriteSuccess(blobsWritten, bytesWritten);
        } else {
            self.Counters.GetTabletCounters()->OnWriteSuccess(blobsWritten, bytesWritten);
        }
        Manager->SaveBlobBatchOnComplete(std::move(BlobBatch));
    } else {
        self.Counters.GetTabletCounters()->OnWriteFailure();
    }
}

void TWriteAction::DoUpdateChannelApproximateFreeSpace(const TUnifiedBlobId& blobId, float approximateFreeSpaceShare) {
    if (!Manager->IsWeightedDataChannelSelectionEnabled()) {
        return;
    }
    auto ev = std::make_unique<NColumnShard::TEvPrivate::TEvUpdateChannelApproximateFreeSpace>(blobId.Channel(), approximateFreeSpaceShare);
    TActorContext::AsActorContext().Send(TabletActorId, ev.release());
}

void TWriteAction::DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    YDB_LOG_INFO("",
        {"event", "write_blob"},
        {"blobId", blobId.ToStringNew()});
    return BlobBatch.SendWriteBlobRequest(data, blobId, TInstant::Max(), TActorContext::AsActorContext());
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
