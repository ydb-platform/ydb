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
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE_INDEX; }

private:
    using TPathIdBlobs = THashMap<ui64, THashSet<TUnifiedBlobId>>;

    TEvPrivate::TEvWriteIndex::TPtr Ev;
    const ui32 TabletTxNo;
    THashMap<TString, TPathIdBlobs> ExportTierBlobs;
    THashMap<TString, THashSet<NOlap::TEvictedBlob>> BlobsToForget;
    ui64 ExportNo = 0;
    TBackgroundActivity TriggerActivity = TBackgroundActivity::All();

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxWriteIndex[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
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

    LOG_S_DEBUG(TxPrefix() << "execute(" << changes->TypeString() << ") changes: " << *changes << TxSuffix());

    bool ok = false;
    if (Ev->Get()->GetPutStatus() == NKikimrProto::OK) {
        NOlap::TSnapshot snapshot(Self->LastPlannedStep, Self->LastPlannedTxId);
        Y_VERIFY(Ev->Get()->IndexInfo.GetLastSchema()->GetSnapshot() <= snapshot);

        TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper dbWrap(txc.DB, &dsGroupSelector);
        ok = Self->TablesManager.MutablePrimaryIndex().ApplyChanges(dbWrap, changes, snapshot); // update changes + apply
        if (ok) {
            LOG_S_DEBUG(TxPrefix() << "(" << changes->TypeString() << ") apply" << TxSuffix());

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
                                LOG_S_ERROR(TxPrefix() << "Prevent evict evicted blob '" << blobId.ToStringNew()
                                    << "'" << TxSuffix());
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
                    if (blobsToDrop.emplace(blobId).second) {
                        LOG_S_TRACE(TxPrefix() << "Delete evicted blob '" << blobId.ToStringNew()
                            << "'" << TxSuffix());
                    }

                }
            }

            Self->IncCounter(COUNTER_PORTIONS_ERASED, changes->PortionsToDrop.size());
            for (const auto& portionInfo : changes->PortionsToDrop) {
                for (const auto& rec : portionInfo.Records) {
                    const auto& blobId = rec.BlobRange.BlobId;
                    if (blobsToDrop.emplace(blobId).second) {
                        LOG_S_TRACE(TxPrefix() << "Delete blob '" << blobId.ToStringNew() << "'" << TxSuffix());
                    }
                }
                Self->IncCounter(COUNTER_RAW_BYTES_ERASED, portionInfo.RawBytesSum());
            }

            for (const auto& blobId : blobsToDrop) {
                if (Self->BlobManager->DropOneToOne(blobId, blobManagerDb)) {
                    TEvictMetadata meta;
                    auto evict = Self->BlobManager->GetDropped(blobId, meta);
                    Y_VERIFY(evict.State != EEvictState::UNKNOWN);

                    BlobsToForget[meta.GetTierName()].emplace(std::move(evict));

                    if (NOlap::IsDeleted(evict.State)) {
                        LOG_S_DEBUG(TxPrefix() << "Skip delete blob '" << blobId.ToStringNew() << "'" << TxSuffix());
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
            LOG_S_NOTICE(TxPrefix() << "(" << changes->TypeString() << ") cannot apply changes: "
                << *changes << TxSuffix());
        }
    } else {
        LOG_S_ERROR(TxPrefix() << " (" << changes->TypeString() << ") cannot write index blobs" << TxSuffix());
    }

    if (blobsToExport.size()) {
        for (auto& [blobId, evFeatures] : blobsToExport) {
            ExportTierBlobs[evFeatures.TargetTierName][evFeatures.PathId].emplace(blobId);
        }
        blobsToExport.clear();

        ui32 numExports = 0;
        for (auto& [tierName, pathBlobs] : ExportTierBlobs) {
            numExports += pathBlobs.size();
        }

        ExportNo = Self->LastExportNo;
        Self->LastExportNo += numExports;

        // Do not start new TTL till we finish current tx. TODO: check if this protection needed
        Y_VERIFY(!Self->ActiveEvictions, "Unexpected active evictions count at tablet %lu", Self->TabletID());
        Self->ActiveEvictions += numExports;

        NIceDb::TNiceDb db(txc.DB);
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastExportNumber, Self->LastExportNo);
    }

    if (changes->IsCleanup()) {
        TriggerActivity = changes->NeedRepeat ? TBackgroundActivity::Cleanup() : TBackgroundActivity::None();
        Self->BlobManager->GetCleanupBlobs(BlobsToForget);
    } else if (changes->IsTtl()) {
        //TriggerActivity = changes->NeedRepeat ? TBackgroundActivity::Ttl() : TBackgroundActivity::None();
    }

    Self->FinishWriteIndex(ctx, Ev, ok, blobsWritten, bytesWritten);
    Self->EnqueueProgressTx(ctx);
    return true;
}

void TTxWriteIndex::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

    if (Ev->Get()->GetPutStatus() == NKikimrProto::TRYLATER) {
        ctx.Schedule(Self->FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
    } else {
        Self->EnqueueBackgroundActivities(false, TriggerActivity);
    }

    for (auto& [tierName, pathBlobs] : ExportTierBlobs) {
        for (auto& [pathId, blobs] : pathBlobs) {
            ++ExportNo;
            Y_VERIFY(pathId);
            auto event = std::make_unique<TEvPrivate::TEvExport>(ExportNo, tierName, pathId, std::move(blobs));
            Self->ExportBlobs(ctx, std::move(event));
        }
        Self->ActiveEvictions -= pathBlobs.size();
    }
    if (ExportTierBlobs.size()) {
        Y_VERIFY(!Self->ActiveEvictions, "Unexpected active evictions count at tablet %lu", Self->TabletID());
    }

    Self->ForgetBlobs(ctx, BlobsToForget);
}

void TColumnShard::FinishWriteIndex(const TActorContext& ctx, TEvPrivate::TEvWriteIndex::TPtr& ev,
                                    bool ok, ui64 blobsWritten, ui64 bytesWritten) {
    auto changes = ev->Get()->IndexChanges;
    Y_VERIFY(changes);

   TablesManager.MutablePrimaryIndex().FreeLocks(changes);

    if (changes->IsInsert()) {
        BackgroundController.FinishIndexing();

        IncCounter(ok ? COUNTER_INDEXING_SUCCESS : COUNTER_INDEXING_FAIL);
        IncCounter(COUNTER_INDEXING_BLOBS_WRITTEN, blobsWritten);
        IncCounter(COUNTER_INDEXING_BYTES_WRITTEN, bytesWritten);
        IncCounter(COUNTER_INDEXING_TIME, ev->Get()->Duration.MilliSeconds());
    } else if (changes->IsCompaction()) {
        Y_VERIFY(changes->CompactionInfo);
        BackgroundController.FinishCompaction(changes->CompactionInfo->GetPlanCompaction());

        if (changes->CompactionInfo->InGranule()) {
            IncCounter(ok ? COUNTER_COMPACTION_SUCCESS : COUNTER_COMPACTION_FAIL);
            IncCounter(COUNTER_COMPACTION_BLOBS_WRITTEN, blobsWritten);
            IncCounter(COUNTER_COMPACTION_BYTES_WRITTEN, bytesWritten);
        } else {
            IncCounter(ok ? COUNTER_SPLIT_COMPACTION_SUCCESS : COUNTER_SPLIT_COMPACTION_FAIL);
            IncCounter(COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, blobsWritten);
            IncCounter(COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, bytesWritten);
        }
        IncCounter(COUNTER_COMPACTION_TIME, ev->Get()->Duration.MilliSeconds());
    } else if (changes->IsCleanup()) {
        BackgroundController.FinishCleanup();

        IncCounter(ok ? COUNTER_CLEANUP_SUCCESS : COUNTER_CLEANUP_FAIL);
    } else if (changes->IsTtl()) {
        BackgroundController.FinishTtl();

        IncCounter(ok ? COUNTER_TTL_SUCCESS : COUNTER_TTL_FAIL);
        IncCounter(COUNTER_EVICTION_BLOBS_WRITTEN, blobsWritten);
        IncCounter(COUNTER_EVICTION_BYTES_WRITTEN, bytesWritten);
    }

    UpdateResourceMetrics(ctx, ev->Get()->ResourceUsage);
}

void TColumnShard::Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
    auto putStatus = ev->Get()->GetPutStatus();

    if (putStatus == NKikimrProto::UNKNOWN) {
        if (IsAnyChannelYellowStop()) {
            LOG_S_ERROR("WriteIndex (out of disk space) at tablet " << TabletID());

            IncCounter(COUNTER_OUT_OF_SPACE);
            ev->Get()->SetPutStatus(NKikimrProto::TRYLATER);
            FinishWriteIndex(ctx, ev);
            ctx.Schedule(FailActivationDelay, new TEvPrivate::TEvPeriodicWakeup(true));
        } else {
            auto& blobs = ev->Get()->Blobs;
            LOG_S_DEBUG("WriteIndex (" << blobs.size() << " blobs) at tablet " << TabletID());

            Y_VERIFY(!blobs.empty());
            ctx.Register(CreateWriteActor(TabletID(), ctx.SelfID,
                BlobManager->StartBlobBatch(), Settings.BlobWriteGrouppingEnabled, ev->Release()));
        }
    } else {
        if (putStatus == NKikimrProto::OK) {
            LOG_S_DEBUG("WriteIndex at tablet " << TabletID());
        } else {
            LOG_S_INFO("WriteIndex error at tablet " << TabletID());
        }

        OnYellowChannels(*ev->Get());
        Execute(new TTxWriteIndex(this, ev), ctx);
    }
}

}
