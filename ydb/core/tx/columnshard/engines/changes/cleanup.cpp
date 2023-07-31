#include "cleanup.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>

namespace NKikimr::NOlap {

void TCleanupColumnEngineChanges::DoDebugString(TStringOutput& out) const {
    if (ui32 dropped = PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : PortionsToDrop) {
            out << portionInfo;
        }
    }
}

void TCleanupColumnEngineChanges::DoWriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    self.IncCounter(NColumnShard::COUNTER_PORTIONS_ERASED, PortionsToDrop.size());
    THashSet<TUnifiedBlobId> blobsToDrop;
    for (const auto& portionInfo : PortionsToDrop) {
        for (const auto& rec : portionInfo.Records) {
            const auto& blobId = rec.BlobRange.BlobId;
            if (blobsToDrop.emplace(blobId).second) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "Delete blob")("blob_id", blobId);
            }
        }
        self.IncCounter(NColumnShard::COUNTER_RAW_BYTES_ERASED, portionInfo.RawBytesSum());
    }

    for (const auto& blobId : blobsToDrop) {
        if (self.BlobManager->DropOneToOne(blobId, *context.BlobManagerDb)) {
            NColumnShard::TEvictMetadata meta;
            auto evict = self.BlobManager->GetDropped(blobId, meta);
            Y_VERIFY(evict.State != EEvictState::UNKNOWN);
            Y_VERIFY(!meta.GetTierName().empty());

            BlobsToForget[meta.GetTierName()].emplace(std::move(evict));

            if (NOlap::IsDeleted(evict.State)) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "SKIP delete blob")("blob_id", blobId);
                continue;
            }
        }
        self.BlobManager->DeleteBlob(blobId, *context.BlobManagerDb);
        self.IncCounter(NColumnShard::COUNTER_BLOBS_ERASED);
        self.IncCounter(NColumnShard::COUNTER_BYTES_ERASED, blobId.BlobSize());
    }
}

bool TCleanupColumnEngineChanges::DoApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context, const bool dryRun) {
    // Drop old portions

    for (auto& portionInfo : PortionsToDrop) {
        if (!self.ErasePortion(portionInfo, !dryRun)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "Cannot erase portion")("portion", portionInfo.DebugString());
            return false;
        }
        if (!dryRun) {
            for (auto& record : portionInfo.Records) {
                self.ColumnsTable->Erase(context.DB, record);
            }
        }
    }

    return true;
}

void TCleanupColumnEngineChanges::DoStart(NColumnShard::TColumnShard& self) {
    self.BackgroundController.StartCleanup();
}

void TCleanupColumnEngineChanges::DoWriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    self.ForgetBlobs(context.ActorContext, BlobsToForget);
    context.TriggerActivity = NeedRepeat ? NColumnShard::TBackgroundActivity::Cleanup() : NColumnShard::TBackgroundActivity::None();
}

void TCleanupColumnEngineChanges::DoOnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& /*context*/) {
    self.BackgroundController.FinishCleanup();
}

NColumnShard::ECumulativeCounters TCleanupColumnEngineChanges::GetCounterIndex(const bool isSuccess) const {
    return isSuccess ? NColumnShard::COUNTER_CLEANUP_SUCCESS : NColumnShard::COUNTER_CLEANUP_FAIL;
}

}
