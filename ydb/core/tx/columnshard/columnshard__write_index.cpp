#include "columnshard_impl.h"
#include "columnshard_txs.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;


bool TTxWriteIndex::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Self->InsertTable);
    Y_VERIFY(Self->PrimaryIndex);

    txc.DB.NoMoreReadsForTx();

    ui64 blobsWritten = 0;
    ui64 bytesWritten = 0;

    auto changes = Ev->Get()->IndexChanges;
    Y_VERIFY(changes);

    bool ok = false;
    if (Ev->Get()->PutStatus == NKikimrProto::OK) {
        NOlap::TSnapshot snapshot = changes->ApplySnapshot;
        if (snapshot.IsZero()) {
            snapshot = {Self->LastPlannedStep, Self->LastPlannedTxId};
        }

        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbWrap(txc.DB, &dsGroupSelector);
        ok = Self->PrimaryIndex->ApplyChanges(dbWrap, changes, snapshot); // update changes + apply
        if (ok) {
            LOG_S_DEBUG("TTxWriteIndex (" << changes->TypeString()
                << ") apply changes: " << *changes << " at tablet " << Self->TabletID());

            TBlobManagerDb blobManagerDb(txc.DB);
            for (const auto& cmtd : changes->DataToIndex) {
                Self->InsertTable->EraseCommitted(dbWrap, cmtd);
                Self->BlobManager->DeleteBlob(cmtd.BlobId, blobManagerDb);
            }
            if (!changes->DataToIndex.empty()) {
                Self->UpdateInsertTableCounters();
            }

            const auto& switchedPortions = changes->SwitchedPortions;
            Self->IncCounter(COUNTER_PORTIONS_DEACTIVATED, switchedPortions.size());

            THashSet<TUnifiedBlobId> blobsDeactivated;
            for (auto& portionInfo : switchedPortions) {
                for (auto& rec : portionInfo.Records) {
                    blobsDeactivated.insert(rec.BlobRange.BlobId);
                }
                Self->IncCounter(COUNTER_RAW_BYTES_DEACTIVATED, portionInfo.RawBytesSum());
            }

            Self->IncCounter(COUNTER_BLOBS_DEACTIVATED, blobsDeactivated.size());
            for (auto& blobId : blobsDeactivated) {
                Self->IncCounter(COUNTER_BYTES_DEACTIVATED, blobId.BlobSize());
            }

            for (auto& portionInfo : changes->AppendedPortions) {
                switch (portionInfo.Meta.Produced) {
                    case NOlap::TPortionMeta::UNSPECIFIED:
                        Y_VERIFY(false); // unexpected
                    case NOlap::TPortionMeta::INSERTED:
                        Self->IncCounter(COUNTER_INDEXING_PORTIONS_WRITTEN);
                        break;
                    case NOlap::TPortionMeta::COMPACTED:
                        Self->IncCounter(COUNTER_COMPACTION_PORTIONS_WRITTEN);
                        break;
                    case NOlap::TPortionMeta::SPLIT_COMPACTED:
                        Self->IncCounter(COUNTER_SPLIT_COMPACTION_PORTIONS_WRITTEN);
                        break;
                    case NOlap::TPortionMeta::EVICTED:
                        Y_FAIL("Unexpected evicted case");
                        break;
                    case NOlap::TPortionMeta::INACTIVE:
                        Y_FAIL("Unexpected inactive case");
                        break;
                }

                // Put newly created blobs into cache
                if (Ev->Get()->CacheData) {
                    for (const auto& columnRec : portionInfo.Records) {
                        const auto* blob = changes->Blobs.FindPtr(columnRec.BlobRange);
                        Y_VERIFY_DEBUG(blob, "Column data must be passed if CacheData is set");
                        if (blob) {
                            Y_VERIFY(columnRec.BlobRange.Size == blob->Size());
                            NBlobCache::AddRangeToCache(columnRec.BlobRange, *blob);
                        }
                    }
                }
            }

            Self->IncCounter(COUNTER_EVICTION_PORTIONS_WRITTEN, changes->PortionsToEvict.size());

            const auto& portionsToDrop = changes->PortionsToDrop;
            THashSet<TUnifiedBlobId> blobsToDrop;
            Self->IncCounter(COUNTER_PORTIONS_ERASED, portionsToDrop.size());
            for (const auto& portionInfo : portionsToDrop) {
                for (const auto& rec : portionInfo.Records) {
                    blobsToDrop.insert(rec.BlobRange.BlobId);
                }
                Self->IncCounter(COUNTER_RAW_BYTES_ERASED, portionInfo.RawBytesSum());
            }

            // Note: RAW_BYTES_ERASED and BYTES_ERASED counters are not in sync for evicted data
            const auto& evictedRecords = changes->EvictedRecords;
            for (const auto& rec : evictedRecords) {
                blobsToDrop.insert(rec.BlobRange.BlobId);
            }

            Self->IncCounter(COUNTER_BLOBS_ERASED, blobsToDrop.size());
            for (const auto& blobId : blobsToDrop) {
                Self->BlobManager->DeleteBlob(blobId, blobManagerDb);
                Self->IncCounter(COUNTER_BYTES_ERASED, blobId.BlobSize());
            }

            blobsWritten = Ev->Get()->BlobBatch.GetBlobCount();
            bytesWritten = Ev->Get()->BlobBatch.GetTotalSize();
            if (blobsWritten) {
                Self->BlobManager->SaveBlobBatch(std::move(Ev->Get()->BlobBatch), blobManagerDb);
            }

            Self->UpdateIndexCounters();
        } else {
            LOG_S_INFO("TTxWriteIndex (" << changes->TypeString()
                << ") cannot apply changes: " << *changes << " at tablet " << Self->TabletID());

            // TODO: delayed insert
        }
    } else {
        LOG_S_ERROR("TTxWriteIndex (" << changes->TypeString()
            << ") cannot write index blobs at tablet " << Self->TabletID());
    }

    if (changes->IsInsert()) {
        Self->ActiveIndexing = false;

        Self->IncCounter(ok ? COUNTER_INDEXING_SUCCESS : COUNTER_INDEXING_FAIL);
        Self->IncCounter(COUNTER_INDEXING_BLOBS_WRITTEN, blobsWritten);
        Self->IncCounter(COUNTER_INDEXING_BYTES_WRITTEN, bytesWritten);
    } else if (changes->IsCompaction()) {
        Self->ActiveCompaction = false;

        Y_VERIFY(changes->CompactionInfo);
        bool inGranule = changes->CompactionInfo->InGranule;

        if (inGranule) {
            Self->IncCounter(ok ? COUNTER_COMPACTION_SUCCESS : COUNTER_COMPACTION_FAIL);
            Self->IncCounter(COUNTER_COMPACTION_BLOBS_WRITTEN, blobsWritten);
            Self->IncCounter(COUNTER_COMPACTION_BYTES_WRITTEN, bytesWritten);
        } else {
            Self->IncCounter(ok ? COUNTER_SPLIT_COMPACTION_SUCCESS : COUNTER_SPLIT_COMPACTION_FAIL);
            Self->IncCounter(COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, blobsWritten);
            Self->IncCounter(COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, bytesWritten);
        }
    } else if (changes->IsCleanup()) {
        Self->ActiveCleanup = false;

        Self->IncCounter(ok ? COUNTER_CLEANUP_SUCCESS : COUNTER_CLEANUP_FAIL);
    } else if (changes->IsTtl()) {
        Self->ActiveTtl = false;

        Self->IncCounter(ok ? COUNTER_TTL_SUCCESS : COUNTER_TTL_FAIL);
        Self->IncCounter(COUNTER_EVICTION_BLOBS_WRITTEN, blobsWritten);
        Self->IncCounter(COUNTER_EVICTION_BYTES_WRITTEN, bytesWritten);
    }

    Self->UpdateResourceMetrics(ctx, Ev->Get()->ResourceUsage);
    return true;
}

void TTxWriteIndex::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxWriteIndex.Complete at tablet " << Self->TabletID());

    if (Ev->Get()->PutStatus == NKikimrProto::TRYLATER) {
        ctx.Schedule(Self->FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
    } else {
        Self->EnqueueBackgroundActivities();
    }
}

}
