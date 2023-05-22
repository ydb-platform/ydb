#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

/// Common transaction for WriteIndex and GranuleCompaction.
/// For WriteIndex it writes new portion from InsertTable into index.
/// For GranuleCompaction it writes new portion of indexed data and mark old data with "switching" snapshot.
class TTxWriteIndex : public TTransactionBase<TColumnShard> {
public:
    TTxWriteIndex(TColumnShard* self, TEvPrivate::TEvWriteIndex::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE_INDEX; }

private:
    struct TPathIdBlobs {
        THashMap<TUnifiedBlobId, TString> Blobs;
        ui64 PathId;
        TPathIdBlobs(const ui64 pathId)
            : PathId(pathId) {

        }
    };

    TEvPrivate::TEvWriteIndex::TPtr Ev;
    THashMap<TString, TPathIdBlobs> ExportTierBlobs;
    THashSet<NOlap::TEvictedBlob> BlobsToForget;
    ui64 ExportNo = 0;
    TBackgroundActivity TriggerActivity = TBackgroundActivity::All();
};


bool TTxWriteIndex::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Self->InsertTable);
    Y_VERIFY(Self->TablesManager.HasPrimaryIndex());

    txc.DB.NoMoreReadsForTx();

    ui64 blobsWritten = 0;
    ui64 bytesWritten = 0;
    THashMap<TUnifiedBlobId, NOlap::TPortionEvictionFeatures> blobsToExport;

    auto changes = Ev->Get()->IndexChanges;
    Y_VERIFY(changes);

    LOG_S_DEBUG("TTxWriteIndex (" << changes->TypeString()
        << ") changes: " << *changes << " at tablet " << Self->TabletID());

    bool ok = false;
    if (Ev->Get()->PutStatus == NKikimrProto::OK) {
        NOlap::TSnapshot snapshot = changes->ApplySnapshot;
        if (snapshot.IsZero()) {
            snapshot = NOlap::TSnapshot(Self->LastPlannedStep, Self->LastPlannedTxId);
            Y_VERIFY(Ev->Get()->IndexInfo.GetLastSchema()->GetSnapshot() <= snapshot);
        }

        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbWrap(txc.DB, &dsGroupSelector);
        ok = Self->TablesManager.MutablePrimaryIndex().ApplyChanges(dbWrap, changes, snapshot); // update changes + apply
        if (ok) {
            LOG_S_DEBUG("TTxWriteIndex (" << changes->TypeString() << ") apply at tablet " << Self->TabletID());

            TBlobManagerDb blobManagerDb(txc.DB);
            for (const auto& cmtd : changes->DataToIndex) {
                Self->InsertTable->EraseCommitted(dbWrap, cmtd);
                Self->BlobManager->DeleteBlob(cmtd.BlobId, blobManagerDb);
                Self->BatchCache.EraseCommitted(cmtd.BlobId);
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

            THashSet<TUnifiedBlobId> protectedBlobs;

            Self->IncCounter(COUNTER_EVICTION_PORTIONS_WRITTEN, changes->PortionsToEvict.size());
            for (auto& [portionInfo, evictionFeatures] : changes->PortionsToEvict) {
                // Mark exported blobs
                if (evictionFeatures.NeedExport) {
                    auto& tierName = portionInfo.TierName;
                    Y_VERIFY(!tierName.empty());

                    for (auto& rec : portionInfo.Records) {
                        auto& blobId = rec.BlobRange.BlobId;
                        if (!blobsToExport.contains(blobId)) {
                            NKikimrTxColumnShard::TEvictMetadata meta;
                            meta.SetTierName(tierName);

                            NOlap::TEvictedBlob evict{
                                .State = EEvictState::EVICTING,
                                .Blob = blobId,
                                .ExternBlob = blobId.MakeS3BlobId(evictionFeatures.PathId)
                            };
                            if (Self->BlobManager->ExportOneToOne(std::move(evict), meta, blobManagerDb)) {
                                blobsToExport.emplace(blobId, evictionFeatures);
                            } else {
                                // TODO: support S3 -> S3 eviction
                                LOG_S_ERROR("Prevent evict evicted blob '" << blobId.ToStringNew()
                                    << "' at tablet " << Self->TabletID());
                                protectedBlobs.insert(blobId);
                            }
                        }
                    }
                }
            }

            // Note: RAW_BYTES_ERASED and BYTES_ERASED counters are not in sync for evicted data
            THashSet<TUnifiedBlobId> blobsToDrop;
            for (const auto& rec : changes->EvictedRecords) {
                const auto& blobId = rec.BlobRange.BlobId;
                if (blobsToExport.contains(blobId)) {
                    // Eviction to S3. TTxExportFinish will delete src blob when dst blob get EEvictState::EXTERN state.
                } else if (!protectedBlobs.contains(blobId)) {
                    // We could drop the blob immediately
                    if (!blobsToDrop.contains(blobId)) {
                        LOG_S_TRACE("Delete evicted blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
                        blobsToDrop.insert(blobId);
                    }

                }
            }

            Self->IncCounter(COUNTER_PORTIONS_ERASED, changes->PortionsToDrop.size());
            for (const auto& portionInfo : changes->PortionsToDrop) {
                for (const auto& rec : portionInfo.Records) {
                    const auto& blobId = rec.BlobRange.BlobId;
                    if (!blobsToDrop.contains(blobId)) {
                        LOG_S_TRACE("Delete blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
                        blobsToDrop.insert(blobId);
                    }
                }
                Self->IncCounter(COUNTER_RAW_BYTES_ERASED, portionInfo.RawBytesSum());
            }

            for (const auto& blobId : blobsToDrop) {
                if (Self->BlobManager->DropOneToOne(blobId, blobManagerDb)) {
                    TEvictMetadata meta;
                    auto evict = Self->BlobManager->GetDropped(blobId, meta);
                    Y_VERIFY(evict.State != EEvictState::UNKNOWN);

                    BlobsToForget.emplace(std::move(evict));

                    if (NOlap::IsDeleted(evict.State)) {
                        LOG_S_DEBUG("Skip delete blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
                        continue;
                    }
                }
                Self->BlobManager->DeleteBlob(blobId, blobManagerDb);
                Self->IncCounter(COUNTER_BLOBS_ERASED);
                Self->IncCounter(COUNTER_BYTES_ERASED, blobId.BlobSize());
            }

            blobsWritten = Ev->Get()->BlobBatch.GetBlobCount();
            bytesWritten = Ev->Get()->BlobBatch.GetTotalSize();
            if (blobsWritten) {
                Self->BlobManager->SaveBlobBatch(std::move(Ev->Get()->BlobBatch), blobManagerDb);
            }

            Self->UpdateIndexCounters();
        } else {
            LOG_S_NOTICE("TTxWriteIndex (" << changes->TypeString()
                << ") cannot apply changes: " << *changes << " at tablet " << Self->TabletID());

            // TODO: delayed insert
        }
    } else {
        LOG_S_ERROR("TTxWriteIndex (" << changes->TypeString()
            << ") cannot write index blobs at tablet " << Self->TabletID());
    }

    if (blobsToExport.size()) {
        size_t numBlobs = blobsToExport.size();
        for (auto& [blobId, evFeatures] : blobsToExport) {
            auto it = ExportTierBlobs.find(evFeatures.TargetTierName);
            if (it == ExportTierBlobs.end()) {
                it = ExportTierBlobs.emplace(evFeatures.TargetTierName, TPathIdBlobs(evFeatures.PathId)).first;
            }
            it->second.Blobs.emplace(blobId, TString());
        }
        blobsToExport.clear();

        ExportNo = Self->LastExportNo + 1;
        Self->LastExportNo += ExportTierBlobs.size();

        LOG_S_DEBUG("TTxWriteIndex init export " << ExportNo << " of " << numBlobs << " blobs in "
            << ExportTierBlobs.size() << " tiers at tablet " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastExportNumber, Self->LastExportNo);
    }

    Self->TablesManager.MutablePrimaryIndex().FreeLocks(changes);

    if (changes->IsInsert()) {
        Self->ActiveIndexing = false;

        Self->IncCounter(ok ? COUNTER_INDEXING_SUCCESS : COUNTER_INDEXING_FAIL);
        Self->IncCounter(COUNTER_INDEXING_BLOBS_WRITTEN, blobsWritten);
        Self->IncCounter(COUNTER_INDEXING_BYTES_WRITTEN, bytesWritten);
        Self->IncCounter(COUNTER_INDEXING_TIME, Ev->Get()->Duration.MilliSeconds());
    } else if (changes->IsCompaction()) {
        Self->ActiveCompaction--;

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
        Self->IncCounter(COUNTER_COMPACTION_TIME, Ev->Get()->Duration.MilliSeconds());
    } else if (changes->IsCleanup()) {
        Self->ActiveCleanup = false;
        TriggerActivity = changes->NeedRepeat ? TBackgroundActivity::Cleanup() : TBackgroundActivity::None();

        Self->BlobManager->GetCleanupBlobs(BlobsToForget);

        Self->IncCounter(ok ? COUNTER_CLEANUP_SUCCESS : COUNTER_CLEANUP_FAIL);
    } else if (changes->IsTtl()) {
        Self->ActiveTtl = false;
        //TriggerActivity = changes->NeedRepeat ? TBackgroundActivity::Ttl() : TBackgroundActivity::None();

        // Do not start new TTL till we evict current PortionsToEvict. We could evict them twice otherwise
        Y_VERIFY(!Self->ActiveEvictions, "Unexpected active evictions count at tablet %lu", Self->TabletID());
        Self->ActiveEvictions = ExportTierBlobs.size();

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
        Self->EnqueueBackgroundActivities(false, TriggerActivity);
    }

    for (auto& [tierName, pathBlobs] : ExportTierBlobs) {
        Y_VERIFY(ExportNo);
        Y_VERIFY(pathBlobs.PathId);

        ctx.Send(Self->SelfId(),
                 new TEvPrivate::TEvExport(ExportNo, tierName, pathBlobs.PathId, std::move(pathBlobs.Blobs)));
        ++ExportNo;
    }

    Self->ForgetBlobs(ctx, BlobsToForget);
}


void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto& blobs = ev->Get()->Blobs;

    if (ev->Get()->PutStatus == NKikimrProto::UNKNOWN) {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("WriteIndex (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->PutStatus = NKikimrProto::TRYLATER;
            Execute(new TTxWriteIndex(this, ev), ctx);
        } else {
            LOG_S_DEBUG("WriteIndex (" << blobs.size() << " blobs) at tablet " << TabletID());

            Y_VERIFY(!blobs.empty());
            ctx.Register(CreateWriteActor(TabletID(), NOlap::TIndexInfo::BuildDefault(), ctx.SelfID,
                BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
        }
    } else {
        if (ev->Get()->PutStatus == NKikimrProto::OK) {
            LOG_S_DEBUG("WriteIndex (records) at tablet " << TabletID());
        } else {
            LOG_S_INFO("WriteIndex error at tablet " << TabletID());
        }

        OnYellowChannels(std::move(ev->Get()->YellowMoveChannels), std::move(ev->Get()->YellowStopChannels));
        Execute(new TTxWriteIndex(this, ev), ctx);
    }
}

}
