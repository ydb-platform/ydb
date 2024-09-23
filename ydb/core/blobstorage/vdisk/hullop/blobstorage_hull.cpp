#include "blobstorage_hull.h"
#include "blobstorage_hullactor.h"
#include "hullop_delayedresp.h"
#include "hullop_compactfreshappendix.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllogcutternotify.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_status.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

template<>
void Out<NKikimr::THullCheckStatus>(IOutputStream &str, const NKikimr::THullCheckStatus &status) {
    str << "{Status# " << status.Status;
    if (status.Postponed) {
        str << " status.Postponed# true Lsn# " << status.Lsn;
    }
    str << "}";
}

namespace NKikimr {

    using TLogoBlobsRunTimeCtx = TLevelIndexRunTimeCtx<TKeyLogoBlob, TMemRecLogoBlob>;
    using TBlocksRunTimeCtx = TLevelIndexRunTimeCtx<TKeyBlock, TMemRecBlock>;
    using TBarriersRunTimeCtx = TLevelIndexRunTimeCtx<TKeyBarrier, TMemRecBarrier>;


    ////////////////////////////////////////////////////////////////////////////
    // THull::TFields
    ////////////////////////////////////////////////////////////////////////////
    struct THull::TFields {
        std::shared_ptr<TLogoBlobsRunTimeCtx> LogoBlobsRunTimeCtx;
        std::shared_ptr<TBlocksRunTimeCtx> BlocksRunTimeCtx;
        std::shared_ptr<TBarriersRunTimeCtx> BarriersRunTimeCtx;
        TIntrusivePtr<TLsnMngr> LsnMngr;
        TActorSystem *ActorSystem;
        const bool BarrierValidation;
        TDelayedResponses DelayedResponses;
        bool AllowGarbageCollection = false;

        TFields(TIntrusivePtr<THullDs> hullDs,
                TIntrusivePtr<TLsnMngr> &&lsnMngr,
                TPDiskCtxPtr &&pdiskCtx,
                const TActorId skeletonId,
                bool runHandoff,
                TActorSystem *as,
                bool barrierValidation)
            : LogoBlobsRunTimeCtx(std::make_shared<TLogoBlobsRunTimeCtx>(lsnMngr, pdiskCtx,
                        skeletonId, runHandoff, hullDs->LogoBlobs))
            , BlocksRunTimeCtx(std::make_shared<TBlocksRunTimeCtx>(lsnMngr, pdiskCtx,
                        skeletonId, runHandoff, hullDs->Blocks))
            , BarriersRunTimeCtx(std::make_shared<TBarriersRunTimeCtx>(lsnMngr, pdiskCtx,
                        skeletonId, runHandoff, hullDs->Barriers))
            , LsnMngr(std::move(lsnMngr))
            , ActorSystem(as)
            , BarrierValidation(barrierValidation)
        {}

        void CutRecoveryLog(const TActorContext &ctx, std::unique_ptr<NPDisk::TEvCutLog> msg) {
            LogoBlobsRunTimeCtx->CutRecoveryLog(ctx, std::unique_ptr<NPDisk::TEvCutLog>(msg->Clone()));
            BlocksRunTimeCtx->CutRecoveryLog(ctx, std::unique_ptr<NPDisk::TEvCutLog>(msg->Clone()));
            BarriersRunTimeCtx->CutRecoveryLog(ctx, std::unique_ptr<NPDisk::TEvCutLog>(msg->Clone()));
        }

        void SetLogNotifierActorId(const TActorId &aid) {
            LogoBlobsRunTimeCtx->SetLogNotifierActorId(aid);
            BlocksRunTimeCtx->SetLogNotifierActorId(aid);
            BarriersRunTimeCtx->SetLogNotifierActorId(aid);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // THull
    ////////////////////////////////////////////////////////////////////////////
    THull::THull(
            TIntrusivePtr<TLsnMngr> lsnMngr,
            TPDiskCtxPtr pdiskCtx,
            const TActorId skeletonId,
            bool runHandoff,
            THullDbRecovery &&uncond,
            TActorSystem *as,
            bool barrierValidation)
        : THullDbRecovery(std::move(uncond))
        , Fields(std::make_unique<TFields>(HullDs, std::move(lsnMngr), std::move(pdiskCtx),
                skeletonId, runHandoff,  as, barrierValidation))
    {}

    THull::~THull() = default;

    ////////////////////////////////////////////////////////////////////////////
    // Private
    ////////////////////////////////////////////////////////////////////////////
    void THull::ValidateWriteQuery(const TActorContext &ctx, const TLogoBlobID &id, bool *writtenBeyondBarrier) {
        if (Fields->BarrierValidation) {
            // ensure that the new blob would not fall under GC
            TString explanation;
            if (!BarrierCache.Keep(id, false, &explanation)) {
                LOG_CRIT(ctx, NKikimrServices::BS_HULLRECS,
                        VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                            "Db# LogoBlobs; putting blob beyond the barrier id# %s barrier# %s",
                            id.ToString().data(), explanation.data()));
                *writtenBeyondBarrier = true;
            }
        }
    }

    TActiveActors THull::RunHullServices(
            TIntrusivePtr<TVDiskConfig> config,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep,
            TActorId loggerId,
            TActorId logCutterId,
            const TActorContext &ctx)
    {
        TActiveActors activeActors;

        // actor for LogCutter Notifier
        auto logNotifierAid = ctx.RegisterWithSameMailbox(CreateHullLogCutterNotifier(hullLogCtx->VCtx, logCutterId, HullDs));
        activeActors.Insert(logNotifierAid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        Fields->SetLogNotifierActorId(logNotifierAid);
        // actor for LogoBlobs DB
        HullDs->LogoBlobs->LIActor = ctx.RegisterWithSameMailbox(CreateLogoBlobsActor(config, HullDs, hullLogCtx, loggerId,
            Fields->LogoBlobsRunTimeCtx, syncLogFirstLsnToKeep));
        activeActors.Insert(HullDs->LogoBlobs->LIActor, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        // actor for Blocks DB
        HullDs->Blocks->LIActor = ctx.RegisterWithSameMailbox(CreateBlocksActor(config, HullDs, hullLogCtx, loggerId,
            Fields->BlocksRunTimeCtx, syncLogFirstLsnToKeep));
        activeActors.Insert(HullDs->Blocks->LIActor, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        // actor for Barriers DB
        HullDs->Barriers->LIActor = ctx.RegisterWithSameMailbox(CreateBarriersActor(config, HullDs, hullLogCtx, loggerId,
            Fields->BarriersRunTimeCtx, syncLogFirstLsnToKeep));
        activeActors.Insert(HullDs->Barriers->LIActor, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

        // create delayed huge blob deleter actor only for LogoBlobs level index as huge blobs are only possible
        // for that data
        auto& deleterInfo = HullDs->LogoBlobs->DelayedCompactionDeleterInfo;
        auto hugeBlobDeleterAid = ctx.RegisterWithSameMailbox(CreateDelayedCompactionDeleterActor(hullLogCtx->HugeKeeperId,
            hullLogCtx->SkeletonId, hullLogCtx->PDiskCtx, hullLogCtx->VCtx, deleterInfo));
        deleterInfo->SetActorId(hugeBlobDeleterAid);
        activeActors.Insert(hugeBlobDeleterAid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

        return activeActors;
    }

    void THull::CutRecoveryLog(const TActorContext &ctx, std::unique_ptr<NPDisk::TEvCutLog> msg) {
        Fields->CutRecoveryLog(ctx, std::move(msg));
    }

    void THull::PostponeReplyUntilCommitted(
            IEventBase *msg,
            const TActorId &recipient,
            ui64 recipientCookie,
            NWilson::TTraceId traceId,
            ui64 lsn)
    {
        Fields->DelayedResponses.Put(msg, recipient, recipientCookie, std::move(traceId), lsn);
    }

    ////////////////////////////////////////////////////////////////////////////
    // LogoBlobs
    ////////////////////////////////////////////////////////////////////////////
    THullCheckStatus THull::CheckLogoBlob(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            bool ignoreBlock,
            const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck>& extraBlockChecks,
            bool *writtenBeyondBarrier)
    {
        // check blocked
        if (!ignoreBlock) {
            auto res = BlocksCache.IsBlocked(id.TabletID(), {id.Generation(), 0});
            switch (res.Status) {
                case TBlocksCache::EStatus::OK:
                    break;
                case TBlocksCache::EStatus::BLOCKED_PERS:
                    return {NKikimrProto::BLOCKED, "blocked", 0, false};
                case TBlocksCache::EStatus::BLOCKED_INFLIGH:
                    return {NKikimrProto::BLOCKED, "blocked", res.Lsn, true};
            }

            for (const auto& item : extraBlockChecks) {
                auto res = BlocksCache.IsBlocked(item.GetTabletId(), {item.GetGeneration(), 0});
                switch (res.Status) {
                    case TBlocksCache::EStatus::OK:
                        break;
                    case TBlocksCache::EStatus::BLOCKED_PERS:
                        return {NKikimrProto::BLOCKED, "blocked", 0, false};
                    case TBlocksCache::EStatus::BLOCKED_INFLIGH:
                        return {NKikimrProto::BLOCKED, "blocked", res.Lsn, true};
                }
            }
        }

        ValidateWriteQuery(ctx, id, writtenBeyondBarrier);
        return {NKikimrProto::OK, 0, false};
    }

    void THull::AddLogoBlob(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            ui8 partId,
            const TIngress &ingress,
            TRope buffer,
            ui64 lsn)
    {
        ReplayAddLogoBlobCmd(ctx, id, partId, ingress, std::move(buffer), lsn, THullDbRecovery::NORMAL);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(HullDs, Fields->LogoBlobsRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    void THull::AddHugeLogoBlob(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            const TIngress &ingress,
            const TDiskPart &diskAddr,
            ui64 lsn)
    {
        ReplayAddHugeLogoBlobCmd(ctx, id, ingress, diskAddr, lsn, THullDbRecovery::NORMAL);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(HullDs, Fields->LogoBlobsRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    void THull::AddLogoBlob(
            const TActorContext &ctx,
            const TLogoBlobID &id,
            const TIngress &ingress,
            const TLsnSeg &seg)
    {
        // don't block osiris logoblobs
        ReplayAddLogoBlobCmd(ctx, id, ingress, seg.Point(), THullDbRecovery::NORMAL);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(HullDs, Fields->LogoBlobsRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    void THull::AddBulkSst(
                const TActorContext &ctx,
                const TAddBulkSstEssence &essence,
                const TLsnSeg &seg)
    {
        ReplayAddBulkSst(ctx, essence, seg, THullDbRecovery::NORMAL);
    }

    ////////////////////////////////////////////////////////////////////////
    // Blocks
    ////////////////////////////////////////////////////////////////////////
    THullCheckStatus THull::CheckBlockCmdAndAllocLsn(ui64 tabletID, ui32 gen, ui64 issuerGuid, ui32 *actGen, TLsnSeg *seg)
    {
        const TBlocksCache::TBlockedGen g(gen, issuerGuid);
        auto res = BlocksCache.IsBlocked(tabletID, g, actGen);
        switch (res.Status) {
            case TBlocksCache::EStatus::OK:
                *actGen = gen;
                // allocate lsn in case of success
                *seg = Fields->LsnMngr->AllocLsnForHullAndSyncLog();
                BlocksCache.UpdateInFlight(tabletID, g, seg->Point());
                return {NKikimrProto::OK, 0, false};
            case TBlocksCache::EStatus::BLOCKED_PERS:
                return {NKikimrProto::ALREADY, "already got", 0, false};
            case TBlocksCache::EStatus::BLOCKED_INFLIGH:
                return {NKikimrProto::ALREADY, "already got", res.Lsn, true};
        }
    }

    void THull::AddBlockCmd(
            const TActorContext &ctx,
            ui64 tabletID,
            ui32 gen,
            ui64 issuerGuid,
            ui64 lsn,
            const THull::TReplySender &replySender)
    {
        // unconditional replay
        ReplayAddBlockCmd(ctx, tabletID, gen, issuerGuid, lsn, THullDbRecovery::NORMAL);
        Fields->DelayedResponses.ConfirmLsn(lsn, replySender);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyBlock, TMemRecBlock>(HullDs, Fields->BlocksRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    ////////////////////////////////////////////////////////////////////////
    // GC
    ////////////////////////////////////////////////////////////////////////
    // check if the barrier key is from the same sequence (identified by Tablet ID, Channel, and Hard fields)
    // as pointed by the iterator (in case if iterator is valid)
    template<typename TIterator>
    static bool IsFromSameSequence(const TKeyBarrier& key, const TIterator& iter) {
        if (iter.Valid()) {
            const TKeyBarrier& iterKey = iter.GetCurKey();
            return key.TabletId == iterKey.TabletId && key.Channel == iterKey.Channel && key.Hard == iterKey.Hard;
        } else {
            return false;
        }
    }

    // validate GC barrier command against existing barriers metabase (ensure that keys are coming in ascending
    // order, CollectGen/CollectStep pairs do not decrease and that keys do not repeat)
    NKikimrProto::EReplyStatus THull::ValidateGCCmd(
            ui64 tabletID,
            ui32 channel,
            bool hard,
            ui32 recordGeneration,
            ui32 perGenCounter,
            ui32 collectGeneration,
            ui32 collectStep,
            const TBarrierIngress& ingress,
            const TActorContext& ctx)
    {
        // check barrier against current state of Hull:
        // 1. If setting in the past => NKikimrProto::ERROR
        // 2. If setting the same => NKikimrProto::ALREADY

        TKeyBarrier newKey(tabletID, channel, recordGeneration, perGenCounter, hard);
        auto snapshot = HullDs->Barriers->GetIndexSnapshot();
        TBarriersSnapshot::TIndexForwardIterator it(HullDs->HullCtx, &snapshot);
        it.Seek(newKey);

        // check if we already have an entry with the same key; ensure that the value is the same as the new one
        // in this case
        if (it.Valid() && it.GetCurKey() == newKey) {
            const TMemRecBarrier &memRec = it.GetMemRec();

            if (memRec.CollectGen != collectGeneration || memRec.CollectStep != collectStep) {
                // we have received GC command with the same key as the existing one, but with different barrier
                // value -- this is not tolerable
                LOG_CRIT(ctx, NKikimrServices::BS_HULLRECS,
                    VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                           "Db# Barriers ValidateGCCmd: incorrect collect cmd: tabletID# %" PRIu64
                            " key# %s existing barrier# %" PRIu32 ":%" PRIu32 " new barrier# %"
                            PRIu32 ":%" PRIu32, tabletID, newKey.ToString().data(), memRec.CollectGen, memRec.CollectStep,
                            collectGeneration, collectStep));
                return NKikimrProto::ERROR;
            }

            // we already have this key with the same value, so we issue ALREADY response
            const TBarrierIngress existingIngress(it.GetMemRec().Ingress);
            TBarrierIngress newIngress = existingIngress;
            TBarrierIngress::Merge(newIngress, ingress);
            return existingIngress.Raw() == newIngress.Raw()
                ? NKikimrProto::ALREADY // if this command is already seen by this disk
                : NKikimrProto::OK; // process command
        }

        // ensure that keys are coming in strictly ascending order
        if (IsFromSameSequence(newKey, it)) {
            // check that existing key is really greater that the new one -- this is internal consistency check
            // so we can do it in Y_ABORT_UNLESS
            const TKeyBarrier& key = it.GetCurKey();
            Y_ABORT_UNLESS(std::make_tuple(key.Gen, key.GenCounter) > std::make_tuple(newKey.Gen, newKey.GenCounter) &&
                    key.TabletId == newKey.TabletId && key.Channel == newKey.Channel && key.Hard == newKey.Hard);

            // we have the key from the same Tablet/Channel, but which is greater than the new one; this
            // means that keys came out-of-order, this key is from the past -- error condition
            LOG_ERROR(ctx, NKikimrServices::BS_HULLRECS,
                 VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                        "Db# Barriers ValidateGCCmd: out-of-order requests:"
                        " existing key# %s new key# %s new barrier# %" PRIu32 ":%" PRIu32, key.ToString().data(),
                        newKey.ToString().data(), collectGeneration, collectStep));
            return NKikimrProto::ERROR;
        }

        // now lets check that CollectGen/CollectStep are coming in nondecreasing order relative to previous
        // key, if any
        TBarriersSnapshot::TIndexBackwardIterator backIt(HullDs->HullCtx, &snapshot);
        backIt.Seek(newKey);
        if (IsFromSameSequence(newKey, backIt)) {
            // check that existing key is strictly less than the new one -- this is also internal consistency check
            const TKeyBarrier& key = backIt.GetCurKey();
            Y_ABORT_UNLESS(std::make_tuple(key.Gen, key.GenCounter) < std::make_tuple(newKey.Gen, newKey.GenCounter) &&
                    key.TabletId == newKey.TabletId && key.Channel == newKey.Channel && key.Hard == newKey.Hard);

            // we have some records in the same tablet id/channel as the new one, so we have to check the order
            const TMemRecBarrier& memRec = backIt.GetMemRec();

            const auto& existingGenStep = std::make_tuple(memRec.CollectGen, memRec.CollectStep);
            const auto& newGenStep = std::make_tuple(collectGeneration, collectStep);

            if (newGenStep < existingGenStep) {
                // we have a command with greater key than the existing one, but the value has decreased
                LOG_CRIT(ctx, NKikimrServices::BS_HULLRECS,
                     VDISKP(HullDs->HullCtx->VCtx->VDiskLogPrefix,
                            "Db# Barriers ValidateGCCmd: decreasing barrier:"
                            " existing key# %s barrier# %" PRIu32 ":%" PRIu32 " new key# %s barrier# %" PRIu32 ":%"
                            PRIu32, key.ToString().data(), memRec.CollectGen, memRec.CollectStep, newKey.ToString().data(),
                            collectGeneration, collectStep));
                return NKikimrProto::ERROR;
            }
        }

        return NKikimrProto::OK;
    }

    THullCheckStatus THull::CheckGCCmdAndAllocLsn(
            const TActorContext &ctx,
            const NKikimrBlobStorage::TEvVCollectGarbage &record,
            const TBarrierIngress &ingress,
            TLsnSeg *seg)
    {
        const ui64 tabletID = record.GetTabletId();
        const ui32 recordGeneration = record.GetRecordGeneration();
        const ui32 perGenCounter = record.GetPerGenerationCounter();
        const bool collect = record.HasCollectGeneration();
        const ui32 collectGeneration = record.GetCollectGeneration();
        const ui32 collectStep = record.GetCollectStep();
        const bool completeDel = NGc::CompleteDelCmd(collectGeneration, collectStep);

        if (!CheckGC(ctx, record))
            return {NKikimrProto::ERROR, 0, false}; // record has duplicates

        auto blockStatus = THullDbRecovery::IsBlocked(record);
        switch (blockStatus.Status) {
            case TBlocksCache::EStatus::OK:
                break;
            case TBlocksCache::EStatus::BLOCKED_PERS:
                return {NKikimrProto::BLOCKED, "blocked", 0, false};
            case TBlocksCache::EStatus::BLOCKED_INFLIGH:
                return {NKikimrProto::BLOCKED, "blocked", blockStatus.Lsn, true};
        }

        // check per generation counter
        if (!record.HasPerGenerationCounter())
            return {NKikimrProto::ERROR, "missing per generation counter"}; // all commands must have per generation counter by contract

        if (completeDel) {
            if (recordGeneration != Max<ui32>() || perGenCounter != Max<ui32>())
                return {NKikimrProto::ERROR, "incorrect complete deletion command"}; // complete del must obey format
        }

        // check keep/don't keep flags and if they are allowed
        const bool hasFlagsButNotAllowed = !HullDs->HullCtx->AllowKeepFlags
            && (record.KeepSize() > 0 || record.DoNotKeepSize() > 0);
        if (hasFlagsButNotAllowed)
            return {NKikimrProto::ERROR, "no keep flags allowed"};

        if (collect) {
            const ui32 channel = record.GetChannel();
            const bool hard = record.GetHard();
            NKikimrProto::EReplyStatus status = ValidateGCCmd(tabletID, channel, hard, recordGeneration,
                    perGenCounter, collectGeneration, collectStep, ingress, ctx);
            if (status != NKikimrProto::OK) {
                return {status, "command invalid"};
            }
        }

        // No more errors, allocate Lsn
        // This record gets one LSN from recovery log's point of view. But for SyncLog we will write several
        // records, one for every KeepFlag, one for every DoNotKeepFlag, and one for the barrier. For this purpose
        // we reserve a diapason of LSNs.
        ui64 lsnAdvance = !!collect + record.KeepSize() + record.DoNotKeepSize();
        Y_ABORT_UNLESS(lsnAdvance > 0);
        *seg = Fields->LsnMngr->AllocLsnForHullAndSyncLog(lsnAdvance);
        return {NKikimrProto::OK, {}};
    }

    void THull::AddGCCmd(
            const TActorContext &ctx,
            const NKikimrBlobStorage::TEvVCollectGarbage &record,
            const TBarrierIngress &ingress,
            const TLsnSeg &seg)
    {
        ReplayAddGCCmd(ctx, record, ingress, seg.Last);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(HullDs, Fields->LogoBlobsRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
        CompactFreshSegmentIfRequired<TKeyBarrier, TMemRecBarrier>(HullDs, Fields->BarriersRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    void THull::CollectPhantoms(
            const TActorContext& /*ctx*/,
            const TDeque<TLogoBlobID>& phantoms,
            const TLsnSeg &seg)
    {
        TIngress ingress;
        ingress.SetKeep(TIngress::IngressMode(HullDs->HullCtx->VCtx->Top->GType), CollectModeDoNotKeep);
        ui64 idLsn = seg.First;
        for (const TLogoBlobID& id : phantoms) {
            HullDs->LogoBlobs->PutToFresh(idLsn, TKeyLogoBlob(id), TMemRecLogoBlob(ingress));
            ++idLsn;
        }
        Y_ABORT_UNLESS(idLsn == seg.Last + 1);
    }

    TLsnSeg THull::AllocateLsnForPhantoms(const TDeque<TLogoBlobID>& phantoms) {
        ui64 lsnAdvance = phantoms.size();
        Y_ABORT_UNLESS(lsnAdvance > 0);
        return Fields->LsnMngr->AllocLsnForHullAndSyncLog(lsnAdvance);
    }

    bool THull::FastKeep(const TLogoBlobID& id, bool keepByIngress, TString *explanation) const {
        return BarrierCache.Keep(id, keepByIngress, explanation);
    }

    ///////////////// STATUS REQUEST ////////////////////////////////////////////
    void THull::StatusRequest(const TActorContext &ctx, TEvLocalStatusResult *result) {
        Y_UNUSED(ctx);
        NKikimrBlobStorage::THullDbStatus *logoBlobStatus = result->Record.MutableLogoBlobs();
        logoBlobStatus->SetCompacted(HullDs->LogoBlobs->FullyCompacted());

        NKikimrBlobStorage::THullDbStatus *blocksStatus = result->Record.MutableBlocks();
        blocksStatus->SetCompacted(HullDs->Blocks->FullyCompacted());

        NKikimrBlobStorage::THullDbStatus *barriersStatus = result->Record.MutableBarriers();
        barriersStatus->SetCompacted(HullDs->Barriers->FullyCompacted());
    }

    ///////////////// SYNC //////////////////////////////////////////////////////
    TLsnSeg THull::AllocateLsnForSyncDataCmd(const TString &data) {
        NSyncLog::TFragmentReader fragment(data);

        // allocate LsnSeg; we reserve a diapason of lsns since we put multiple records
        std::vector<const NSyncLog::TRecordHdr*> records = fragment.ListRecords();
        ui64 lsnAdvance = records.size();
        Y_ABORT_UNLESS(lsnAdvance > 0);
        auto seg = Fields->LsnMngr->AllocLsnForHull(lsnAdvance);

        // update blocks cache by blocks that are in flight
        ui64 curLsn = seg.First;
        auto blockHandler = [&] (const NSyncLog::TBlockRec *rec) {
            BlocksCache.UpdateInFlight(rec->TabletId, {rec->Generation, 0}, curLsn++);
        };
        auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2 *rec) {
            BlocksCache.UpdateInFlight(rec->TabletId, {rec->Generation, rec->IssuerGuid}, curLsn++);
        };
        auto otherHandler = [&] (const void *) {
            curLsn++;
        };
        // do job - update blocks cache
        for (const NSyncLog::TRecordHdr* rec : records) {
            NSyncLog::HandleRecordHdr(rec, otherHandler, blockHandler, otherHandler, blockHandlerV2);
        }
        // check that all records are applied
        Y_DEBUG_ABORT_UNLESS(curLsn == seg.Last + 1);

        return seg;
    }

    TLsnSeg THull::AllocateLsnForSyncDataCmd(const TFreshBatch &freshBatch) {
        const ui64 logoBlobsCount = freshBatch.LogoBlobs ? freshBatch.LogoBlobs->GetSize() : 0;
        const ui64 blocksCount = freshBatch.Blocks ? freshBatch.Blocks->GetSize() : 0;
        const ui64 barriersCount = freshBatch.Barriers ? freshBatch.Barriers->GetSize() : 0;
        const ui64 lsnAdvance = logoBlobsCount + blocksCount + barriersCount;

        // allocate LsnSeg; we reserve a diapason of lsns since we put multiple records
        Y_ABORT_UNLESS(lsnAdvance > 0);
        auto seg = Fields->LsnMngr->AllocLsnForHull(lsnAdvance);

        if (freshBatch.Blocks) {
            ui64 curLsn = seg.First + logoBlobsCount;
            TFreshAppendixBlocks::TIterator it(HullDs->HullCtx, freshBatch.Blocks.get());
            it.SeekToFirst();
            while (it.Valid()) {
                BlocksCache.UpdateInFlight(it.GetCurKey().TabletId, {it.GetMemRec().BlockedGeneration, 0}, curLsn++);
                it.Next();
            }
        }

        return seg;
    }

    void THull::AddSyncDataCmd(
            const TActorContext &ctx,
            const TString &data,
            const TLsnSeg &seg,
            const THull::TReplySender &replySender)
    {
        ui64 curLsn = seg.First;

        // record handlers
        auto blobHandler = [&] (const NSyncLog::TLogoBlobRec *rec) {
            Y_DEBUG_ABORT_UNLESS(TIngress::MustKnowAboutLogoBlob(HullDs->HullCtx->VCtx->Top.get(),
                                                           HullDs->HullCtx->VCtx->ShortSelfVDisk,
                                                           rec->LogoBlobID()),
                         "logoBlobID# %s ShortSelfVDisk# %s top# %s",
                           rec->LogoBlobID().ToString().data(),
                           HullDs->HullCtx->VCtx->ShortSelfVDisk.ToString().data(),
                           HullDs->HullCtx->VCtx->Top->ToString().data());
            TLogoBlobID genId(rec->LogoBlobID(), 0); // TODO: add verify for logoBlob.PartId() == 0 after migration
            ReplayAddLogoBlobCmd(ctx, genId, rec->Ingress, curLsn++, THullDbRecovery::NORMAL);
        };
        auto blockHandler = [&] (const NSyncLog::TBlockRec *rec) {
            // replay uncondititionally
            ReplayAddBlockCmd(ctx, rec->TabletId, rec->Generation, 0, curLsn, THullDbRecovery::NORMAL);
            Fields->DelayedResponses.ConfirmLsn(curLsn, replySender);
            ++curLsn;
        };
        auto barrierHandler = [&] (const NSyncLog::TBarrierRec *rec) {
            ReplayAddBarrierCmd(ctx, rec->TabletId, rec->Channel, rec->Gen, rec->GenCounter, rec->CollectGeneration,
                rec->CollectStep, rec->Hard, rec->Ingress, curLsn++, THullDbRecovery::NORMAL);
        };
        auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2 *rec) {
            ReplayAddBlockCmd(ctx, rec->TabletId, rec->Generation, rec->IssuerGuid, curLsn, THullDbRecovery::NORMAL);
            Fields->DelayedResponses.ConfirmLsn(curLsn, replySender);
            ++curLsn;
        };

        // process synclog data
        NSyncLog::TFragmentReader fragment(data);
        fragment.ForEach(blobHandler, blockHandler, barrierHandler, blockHandlerV2);
        // check that all records are applied
        Y_DEBUG_ABORT_UNLESS(curLsn == seg.Last + 1);

        // run compaction if required
        CompactFreshSegmentIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(HullDs, Fields->LogoBlobsRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
        CompactFreshSegmentIfRequired<TKeyBlock, TMemRecBlock>(HullDs, Fields->BlocksRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
        CompactFreshSegmentIfRequired<TKeyBarrier, TMemRecBarrier>(HullDs, Fields->BarriersRunTimeCtx, ctx, false,
            Fields->AllowGarbageCollection);
    }

    // run fresh segment or fresh appendix compaction if required
    template <class TKey, class TMemRec>
    void CompactFreshIfRequired(
            const TActorContext &ctx,
            TIntrusivePtr<THullDs> &hullDs,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKey, TMemRec>> &rtCtx,
            TLevelIndex<TKey, TMemRec> &levelIndex,
            bool allowGarbageCollection)
    {
        // try to start fresh compaction
        bool freshSegmentCompaction = CompactFreshSegmentIfRequired<TKey, TMemRec>(hullDs, rtCtx, ctx, false,
            allowGarbageCollection);
        if (!freshSegmentCompaction) {
            // if not, try to start appendix compaction
            auto cjob = levelIndex.CompactFreshAppendix();
            if (!cjob.Empty()) {
                // we don't save actorId, because the actor we born only has Bootstrap method and that's all
                RunFreshAppendixCompaction<TKey, TMemRec>(ctx, hullDs->HullCtx->VCtx,
                        levelIndex.LIActor, std::move(cjob));
            }
        }
    }

    void THull::AddSyncDataCmd(
            const TActorContext &ctx,
            TFreshBatch &&freshBatch,
            const TLsnSeg &seg,
            const TReplySender &replySender)
    {
        ui64 curLsn = seg.First;
        if (freshBatch.LogoBlobs) {
            const ui64 logoBlobsCount = freshBatch.LogoBlobs->GetSize();
            TLsnSeg s(curLsn, curLsn + logoBlobsCount - 1);
            ReplaySyncDataCmd_LogoBlobsBatch(ctx, std::move(freshBatch.LogoBlobs), s, THullDbRecovery::NORMAL);
            curLsn += logoBlobsCount;
            CompactFreshIfRequired<TKeyLogoBlob, TMemRecLogoBlob>(ctx, HullDs,
                    Fields->LogoBlobsRunTimeCtx, *HullDs->LogoBlobs, Fields->AllowGarbageCollection);
        }

        if (freshBatch.Blocks) {
            const ui64 blocksCount = freshBatch.Blocks->GetSize();
            TLsnSeg s(curLsn, curLsn + blocksCount - 1);

            // confirm lsns
            TFreshAppendixBlocks::TIterator it(HullDs->HullCtx, freshBatch.Blocks.get());
            it.SeekToFirst();
            while (it.Valid()) {
                Fields->DelayedResponses.ConfirmLsn(curLsn++, replySender);
                it.Next();
            }

            ReplaySyncDataCmd_BlocksBatch(ctx, std::move(freshBatch.Blocks), s, THullDbRecovery::NORMAL);
            // curLsn already updated in cycle
            CompactFreshIfRequired<TKeyBlock, TMemRecBlock>(ctx, HullDs,
                Fields->BlocksRunTimeCtx, *HullDs->Blocks, Fields->AllowGarbageCollection);
        }

        if (freshBatch.Barriers) {
            const ui64 barriersCount = freshBatch.Barriers->GetSize();
            TLsnSeg s(curLsn, curLsn + barriersCount - 1);
            ReplaySyncDataCmd_BarriersBatch(ctx, std::move(freshBatch.Barriers), s, THullDbRecovery::NORMAL);
            curLsn += barriersCount;
            CompactFreshIfRequired<TKeyBarrier, TMemRecBarrier>(ctx, HullDs,
                Fields->BarriersRunTimeCtx, *HullDs->Barriers, Fields->AllowGarbageCollection);
        }
        Y_ABORT_UNLESS(curLsn == seg.Last + 1);
    }

    ///////////////// GET SNAPSHOT //////////////////////////////////////////////
    THullDsSnap THull::GetSnapshot() const {
        return HullDs->GetSnapshot(Fields->ActorSystem);
    }

    THullDsSnap THull::GetIndexSnapshot() const {
        return HullDs->GetIndexSnapshot();
    }

    void THull::PermitGarbageCollection(const TActorContext& ctx) {
        Fields->AllowGarbageCollection = true;
        ctx.Send(HullDs->LogoBlobs->LIActor, new TEvPermitGarbageCollection);
        ctx.Send(HullDs->Blocks->LIActor, new TEvPermitGarbageCollection);
        ctx.Send(HullDs->Barriers->LIActor, new TEvPermitGarbageCollection);
    }

} // NKikimr
