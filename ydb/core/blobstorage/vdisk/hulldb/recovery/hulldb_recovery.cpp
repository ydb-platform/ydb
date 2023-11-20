#include "hulldb_recovery.h"
#include <google/protobuf/messagext.h>

namespace NKikimr {

    const char *THullDbRecovery::OpMode2Str(EOpMode mode) {
        switch (mode) {
            case RECOVERY:  return "RECOVERY";
            case NORMAL:    return "NORMAL";
            default:        return "UNKNOWN";
        }
    }

    THullDbRecovery::THullDbRecovery(TIntrusivePtr<THullCtx> hullCtx)
        : HullDs(new THullDs(hullCtx))
        , Filter(hullCtx->VCtx->Top, hullCtx->VCtx->ShortSelfVDisk)
    {}

    ///////////////// LOGOBLOBS /////////////////////////////////////////////
    void THullDbRecovery::ReplayAddLogoBlobCmd(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            ui8 partId,
            const TIngress &ingress,
            TRope buffer,
            ui64 lsn,
            EOpMode mode)
    {
        Y_ABORT_UNLESS(!id.PartId());
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# LogoBlobs action# add_data mode# %s id# %s lsn# %" PRIu64 " bufSize# %" PRIu32,
                    OpMode2Str(mode), id.ToString().data(), lsn, ui32(buffer.GetSize())));

        HullDs->LogoBlobs->PutToFresh(lsn, TKeyLogoBlob(id), partId, ingress, std::move(buffer));
    }

    void THullDbRecovery::ReplayAddLogoBlobCmd(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            const TIngress &ingress,
            ui64 lsn,
            EOpMode mode)
    {
        Y_ABORT_UNLESS(!id.PartId());
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# LogoBlobs action# add_idx mode# %s id# %s lsn# %" PRIu64,
                    OpMode2Str(mode), id.ToString().data(), lsn));

        HullDs->LogoBlobs->PutToFresh(lsn, TKeyLogoBlob(id), TMemRecLogoBlob(ingress));
    }

    void THullDbRecovery::ReplayAddHugeLogoBlobCmd(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            const TIngress &ingress,
            const TDiskPart &diskAddr,
            ui64 lsn,
            EOpMode mode)
    {
        Y_ABORT_UNLESS(!id.PartId());
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# LogoBlobs action# add_data_huge mode# %s id# %s lsn# %" PRIu64,
                    OpMode2Str(mode), id.ToString().data(), lsn));

        TMemRecLogoBlob memRecLogoBlob(ingress);
        memRecLogoBlob.SetHugeBlob(diskAddr);
        HullDs->LogoBlobs->PutToFresh(lsn, TKeyLogoBlob(id), memRecLogoBlob);
    }

    void THullDbRecovery::ReplayAddBulkSst(
            const TActorContext &ctx,
            const TAddBulkSstEssence &essence,
            const TLsnSeg &seg,
            EOpMode mode)
    {
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# LogoBlobs Db# Blocks Db# Barriers action# bulk_sst mode# %s lsn# %" PRIu64 " essence# %s",
                    OpMode2Str(mode), seg.Point(), essence.ToString().data()));

        // FIXME: implement
        //HullDs->LogoBlobs->ApplyUncommittedReplSegment(std::move(sst), HullDs->HullCtx);


        for (const auto &addition : essence.LogoBlobsAdditions) {
            HullDs->LogoBlobs->InsertSstAtLevel0(addition.Sst, HullDs->HullCtx);
        }
        for (const auto &addition : essence.BlocksAdditions) {
            HullDs->Blocks->InsertSstAtLevel0(addition.Sst, HullDs->HullCtx);
        }
        for (const auto &addition : essence.BarriersAdditions) {
            HullDs->Barriers->InsertSstAtLevel0(addition.Sst, HullDs->HullCtx);
        }
    }

    ///////////////// BLOCKS ////////////////////////////////////////////////
    void THullDbRecovery::ReplayAddBlockCmd(
            const TActorContext &ctx,
            ui64 tabletId,
            ui32 gen,
            ui64 issuerGuid,
            ui64 lsn,
            EOpMode mode)
    {
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# Blocks action# add mode# %s tabletId# %" PRIu64
                    " gen# %" PRIu32 " lsn# %" PRIu64, OpMode2Str(mode), tabletId, gen, lsn));

        UpdateBlocksCache(tabletId, gen, issuerGuid, lsn, mode);
        HullDs->Blocks->PutToFresh(lsn, TKeyBlock(tabletId), TMemRecBlock(gen));
    }

    void THullDbRecovery::UpdateBlocksCache(ui64 tabletId, ui32 gen, ui64 issuerGuid, ui64 lsn, EOpMode mode) {
        switch (mode) {
            case THullDbRecovery::NORMAL:
                BlocksCache.CommitInFlight(tabletId, {gen, issuerGuid}, lsn);
                break;
            case THullDbRecovery::RECOVERY:
                // Update cache of blocks; we update it even when we replay log as we preinitialize
                // cache only with SST's
                BlocksCache.UpdatePersistent(tabletId, {gen, issuerGuid});
                break;
        }
    }

    void THullDbRecovery::UpdateBlocksCache(
            const std::shared_ptr<TFreshAppendixBlocks> &blocks,
            TLsnSeg seg,
            EOpMode mode)
    {
        Y_DEBUG_ABORT_UNLESS(blocks);

        // update blocks cache with newly inserted elements
        TFreshAppendixBlocks::TIterator it(HullDs->HullCtx, blocks.get());
        it.SeekToFirst();
        ui64 lsnIt = seg.First;
        while (it.Valid()) {
            // TODO(alexvru): correct handling of fresh appendix with blocks
            UpdateBlocksCache(it.GetCurKey().TabletId, it.GetMemRec().BlockedGeneration, 0, lsnIt++, mode);
            it.Next();
        }
        Y_ABORT_UNLESS(seg.Last + 1 == lsnIt);
    }

    ///////////////// GC ////////////////////////////////////////////////////
    void THullDbRecovery::ReplayAddBarrierCmd(
            const TActorContext &ctx,
            ui64 tabletID,
            ui32 channel,
            ui32 gen,
            ui32 genCounter,
            ui32 collectGen,
            ui32 collectStep,
            bool hard,
            const TBarrierIngress &ingress,
            ui64 lsn,
            EOpMode mode)
    {
        LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                    "Db# Barriers action# add mode# %s tabletID# %" PRIu64
                    " channel# %" PRIu32 " gen# %" PRIu32
                    " genCounter# %" PRIu32 " collectGen# %" PRIu32
                    " collectStep# %" PRIu32 " hard# %s lsn# %" PRIu64,
                    OpMode2Str(mode), tabletID, channel, gen, genCounter, collectGen, collectStep,
                    hard ? "true" : "false", lsn));

        TKeyBarrier keyBarrier(tabletID, channel, gen, genCounter, hard);
        TMemRecBarrier memRecBarrier(collectGen, collectStep, ingress);
        HullDs->Barriers->PutToFresh(lsn, keyBarrier, memRecBarrier);

        if (HullDs->HullCtx->BarrierValidation) {
            UpdateBarrierCache(keyBarrier);
        }
    }

    void THullDbRecovery::UpdateBarrierCache(const TKeyBarrier& key) {
        // take a snapshot of the database including new record's LSN
        TBarriersSnapshot snapshot(HullDs->Barriers->GetIndexSnapshot());

        // create an iterator and seek to the just added key
        TBarriersSnapshot::TForwardIterator it(HullDs->HullCtx, &snapshot);
        it.Seek(key);

        // ensure that everything is correct
        Y_ABORT_UNLESS(it.Valid() && it.GetCurKey() == key);

        // put it into merger
        TIndexRecordMerger<TKeyBarrier, TMemRecBarrier> merger(HullDs->HullCtx->VCtx->Top->GType);
        it.PutToMerger(&merger);
        merger.Finish();
        const TMemRecBarrier& memRec = merger.GetMemRec();

        // check if the record has quorum
        if (!HullDs->HullCtx->GCOnlySynced || memRec.Ingress.IsQuorum(HullDs->HullCtx->IngressCache.Get()) || key.Hard) {
            // apply new record to the barrier cache
            BarrierCache.Update(key.TabletId, key.Channel, key.Hard, memRec.CollectGen, memRec.CollectStep);
        }
    }

    void THullDbRecovery::UpdateBarrierCache(const std::shared_ptr<TFreshAppendixBarriers>& barriers) {
        Y_DEBUG_ABORT_UNLESS(barriers);

        // update barriers cache with newly inserted elements
        TFreshAppendixBarriers::TIterator it(HullDs->HullCtx, barriers.get());
        it.SeekToFirst();
        while (it.Valid()) {
            UpdateBarrierCache(it.GetCurKey());
            it.Next();
        }
    }

    // function checks incoming garbage collection message for duplicates
    bool THullDbRecovery::CheckGC(const TActorContext &ctx, const NKikimrBlobStorage::TEvVCollectGarbage &record) {
        TVector<TLogoBlobID> vec;
        vec.reserve(record.KeepSize() + record.DoNotKeepSize());

        for (ui32 i = 0; i < record.KeepSize(); ++i) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetKeep(i));
            Y_ABORT_UNLESS(id.PartId() == 0);
            vec.push_back(id);
        }

        for (ui32 i = 0; i < record.DoNotKeepSize(); ++i) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetDoNotKeep(i));
            Y_ABORT_UNLESS(id.PartId() == 0);
            vec.push_back(id);
        }

        if (vec.size() < 2)
            return true;

        Sort(vec.begin(), vec.end());

        TLogoBlobID id(vec[0]);
        for (ui32 i = 1; i < vec.size(); ++i) {
            if (id == vec[i]) {
                LOG_CRIT(ctx, NKikimrServices::BS_HULLRECS,
                        VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                            "Duplicates blob: %s in TEvVCollectGarbage: %s",
                            ToString(id).data(), ShortUtf8DebugString(record).data()));
                return false;
            }

            id = vec[i];
        }

        return true;
    }

    TBlocksCache::TBlockRes THullDbRecovery::IsBlocked(const NKikimrBlobStorage::TEvVCollectGarbage &record) const {
        const ui64 tabletID = record.GetTabletId();
        const ui32 recordGeneration = record.GetRecordGeneration();
        const ui32 collectGeneration = record.GetCollectGeneration();
        const ui32 collectStep = record.GetCollectStep();
        const bool completeDel = NGc::CompleteDelCmd(collectGeneration, collectStep);

        // check blocked
        ui32 actualGen = 0;
        auto res = BlocksCache.IsBlocked(tabletID, {recordGeneration, 0}, &actualGen);
        switch (res.Status) {
            case TBlocksCache::EStatus::OK:
                return res;
            case TBlocksCache::EStatus::BLOCKED_PERS:
            case TBlocksCache::EStatus::BLOCKED_INFLIGH:
                if (completeDel && actualGen == Max<ui32>()) {
                    // Complete table deletion command -> OK
                    return {TBlocksCache::EStatus::OK, 0};
                } else {
                    // BLOCKED_PERS or BLOCKED_INFLIGH, depending on res
                    return res;
                }
        }
    }

    void THullDbRecovery::ReplayAddGCCmd(
            const TActorContext &ctx,
            const NKikimrBlobStorage::TEvVCollectGarbage &record,
            const TBarrierIngress &ingress,
            ui64 lsn)
    {
        ReplayAddGCCmd_BarrierSubcommand(ctx, record, ingress, lsn, THullDbRecovery::NORMAL);
        ReplayAddGCCmd_LogoBlobsSubcommand(ctx, record, lsn, THullDbRecovery::NORMAL);
    }

    void THullDbRecovery::ReplayAddGCCmd_BarrierSubcommand(
            const TActorContext &ctx,
            const NKikimrBlobStorage::TEvVCollectGarbage &record,
            const TBarrierIngress &ingress,
            ui64 lsn,
            EOpMode mode) {

        const bool collect = record.HasCollectGeneration();
        if (collect) {
            const ui64 tabletID = record.GetTabletId();
            const ui32 channel = record.GetChannel();
            const ui32 gen = record.GetRecordGeneration();
            const ui32 genCounter = record.GetPerGenerationCounter();
            const ui32 collectGeneration = record.GetCollectGeneration();
            const ui32 collectStep = record.GetCollectStep();
            const bool hard = record.GetHard();

            ReplayAddBarrierCmd(ctx, tabletID, channel, gen, genCounter, collectGeneration, collectStep, hard,
                    ingress, lsn, mode);
        }
    }

    void THullDbRecovery::ReplayAddGCCmd_LogoBlobsSubcommand(
            const TActorContext &ctx,
            const NKikimrBlobStorage::TEvVCollectGarbage &record,
            ui64 lsn,
            EOpMode mode)
    {
        if (mode == RECOVERY) {
            // we check at RECOVERY mode only, because incoming message is being checked early
            Y_ABORT_UNLESS(CheckGC(ctx, record)); // TODO: CheckGC consume resources just to be sure incoming message is good
        }

        // set up keep bits
        TIngress ingressKeep;
        ingressKeep.SetKeep(TIngress::IngressMode(HullDs->HullCtx->VCtx->Top->GType), CollectModeKeep);
        for (ui32 i = 0; i < record.KeepSize(); ++i) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetKeep(i));
            Y_ABORT_UNLESS(id.PartId() == 0);
            // Put only those logoblobs that belong to this vdisk
            if (Filter.Check(id)) {
                TKeyLogoBlob key(id);
                TMemRecLogoBlob memRec(ingressKeep);
                HullDs->LogoBlobs->PutToFresh(lsn, key, memRec);
                LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                        VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                            "Db# LogoBlobs action# set_keep mode# %s id# %s lsn# %" PRIu64,
                            OpMode2Str(mode), id.ToString().data(), lsn));
            }
        }

        // set up do not keep bits
        TIngress ingressDontKeep;
        ingressDontKeep.SetKeep(TIngress::IngressMode(HullDs->HullCtx->VCtx->Top->GType), CollectModeDoNotKeep);
        for (ui32 i = 0; i < record.DoNotKeepSize(); ++i) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetDoNotKeep(i));
            Y_ABORT_UNLESS(id.PartId() == 0);
            if (Filter.Check(id)) {
                TKeyLogoBlob key(id);
                TMemRecLogoBlob memRec(ingressDontKeep);
                HullDs->LogoBlobs->PutToFresh(lsn, key, memRec);
                LOG_DEBUG(ctx, NKikimrServices::BS_HULLRECS,
                        VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                            "Db# LogoBlobs action# set_dont_keep mode# %s id# %s lsn# %" PRIu64,
                            OpMode2Str(mode), id.ToString().data(), lsn));
            }
        }
    }

    void THullDbRecovery::ReplaySyncDataCmd_LogoBlobsBatch(
            const TActorContext &ctx,
            std::shared_ptr<TFreshAppendixLogoBlobs> &&logoBlobs,
            TLsnSeg seg,
            EOpMode mode)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLRECS, HullDs->HullCtx->VCtx->VDiskLogPrefix
                << "Db# LogoBlobs action# sync_data_batch mode# " << OpMode2Str(mode) << " lsn# " << seg);

        Y_ABORT_UNLESS(logoBlobs && (seg.Last - seg.First + 1 == logoBlobs->GetSize()));
        HullDs->LogoBlobs->PutToFresh(std::move(logoBlobs), seg.First, seg.Last);
    }

    void THullDbRecovery::ReplaySyncDataCmd_BlocksBatch(
            const TActorContext &ctx,
            std::shared_ptr<TFreshAppendixBlocks> &&blocks,
            TLsnSeg seg,
            EOpMode mode)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLRECS, HullDs->HullCtx->VCtx->VDiskLogPrefix
                << "Db# Blocks action# sync_data_batch mode# " << OpMode2Str(mode) << " lsn# " << seg);

        Y_ABORT_UNLESS(blocks && (seg.Last - seg.First + 1 == blocks->GetSize()));
        UpdateBlocksCache(blocks, seg, mode);
        HullDs->Blocks->PutToFresh(std::move(blocks), seg.First, seg.Last);
    }

    void THullDbRecovery::ReplaySyncDataCmd_BarriersBatch(
            const TActorContext &ctx,
            std::shared_ptr<TFreshAppendixBarriers> &&barriers,
            TLsnSeg seg,
            EOpMode mode)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLRECS, HullDs->HullCtx->VCtx->VDiskLogPrefix
                << "Db# Barriers action# sync_data_batch mode# " << OpMode2Str(mode) << " lsn# " << seg);

        Y_ABORT_UNLESS(barriers && (seg.Last - seg.First + 1 == barriers->GetSize()));
        if (HullDs->HullCtx->BarrierValidation) {
            UpdateBarrierCache(barriers);
        }
        HullDs->Barriers->PutToFresh(std::move(barriers), seg.First, seg.Last);
    }

    void THullDbRecovery::GetOwnedChunks(TSet<TChunkIdx>& chunks) const
    {
        HullDs->LogoBlobs->GetOwnedChunks(chunks);
        HullDs->Blocks->GetOwnedChunks(chunks);
        HullDs->Barriers->GetOwnedChunks(chunks);
    }

    void THullDbRecovery::BuildBarrierCache()
    {
        if (HullDs->HullCtx->BarrierValidation) {
            BarrierCache.Build(HullDs.Get());
        }
    }

    void THullDbRecovery::BuildBlocksCache()
    {
        BlocksCache.Build(HullDs.Get());
    }

    TSatisfactionRank THullDbRecovery::GetSatisfactionRank(EHullDbType t, ESatisfactionRankType s) const
    {
        return HullDs->GetSatisfactionRank(t, s);
    }

    void THullDbRecovery::OutputHtmlForDb(IOutputStream &str) const
    {
        if (HullDs) {
            if (HullDs->LogoBlobs)
                HullDs->LogoBlobs->OutputHtml("LogoBlobs", str);
            if (HullDs->Blocks)
                HullDs->Blocks->OutputHtml("Blocks", str);
            if (HullDs->Barriers)
                HullDs->Barriers->OutputHtml("Barriers", str);
        }
    }

    void THullDbRecovery::OutputHtmlForHugeBlobDeleter(IOutputStream &str) const
    {
        if (HullDs && HullDs->LogoBlobs) {
            HullDs->LogoBlobs->DelayedCompactionDeleterInfo->RenderState(str);
        }
    }

    void THullDbRecovery::OutputProtoForDb(NKikimrVDisk::VDiskStat *proto) const {
        if (HullDs) {
            if (HullDs->LogoBlobs) {
                HullDs->LogoBlobs->OutputProto(proto->mutable_logoblobs());
            }
            if (HullDs->Blocks) {
                HullDs->Blocks->OutputProto(proto->mutable_blocks());
            }
            if (HullDs->Barriers) {
                HullDs->Barriers->OutputProto(proto->mutable_barriers());
            }
        }
    }

} // NKikimr
