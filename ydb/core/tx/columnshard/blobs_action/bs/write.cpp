#include "write.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

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
        self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
        self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
        //    self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_RAW_BYTES_UPSERTED, insertedBytes);
        self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_WRITE_SUCCESS);
        Manager->SaveBlobBatchOnComplete(std::move(BlobBatch));
    } else {
        self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_WRITE_FAIL);
    }
}

void TWriteAction::DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "write_blob")("blob_id", blobId.ToStringNew());
    return BlobBatch.SendWriteBlobRequest(data, blobId, TInstant::Max(), TActorContext::AsActorContext());
}

}
