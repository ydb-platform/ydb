#include "bs.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap {

void TBSWriteAction::DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
    ui64 blobsWritten = BlobBatch.GetBlobCount();
    ui64 bytesWritten = BlobBatch.GetTotalSize();
    self.IncCounter(NColumnShard::COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
    self.IncCounter(NColumnShard::COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
//    self.IncCounter(NColumnShard::COUNTER_RAW_BYTES_UPSERTED, insertedBytes);
    self.IncCounter(NColumnShard::COUNTER_WRITE_SUCCESS);
    self.BlobManager->SaveBlobBatch(std::move(BlobBatch), dbBlobs);
}

}
