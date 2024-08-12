#pragma once

#include "defs.h"
#include "blobstorage_hullcompactworker.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/queue.h>


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullChange
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct THullChange : public TEventLocal<THullChange<TKey, TMemRec>, TEvBlobStorage::EvHullChange> {
        typedef ::NKikimr::TFreshSegment<TKey, TMemRec> TFreshSegment;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;

        // some reserved chunks left, we can reuse them
        TVector<ui32> ReservedChunks;
        // chunks to commit
        TVector<ui32> CommitChunks;
        // resulting segments after compaction
        TOrderedLevelSegmentsPtr SegVec;
        // original FreshSegment if any
        TIntrusivePtr<TFreshSegment> FreshSegment;
        // huge blobs to delete after compaction
        TDiskPartVec FreedHugeBlobs;
        // was the compaction process aborted by some reason?
        bool Aborted = false;

        THullChange() = default;
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullCompaction
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, class TIterator>
    class THullCompaction : public TActorBootstrapped<THullCompaction<TKey, TMemRec, TIterator>> {

        typedef ::NKikimr::TLevelIndex<TKey, TMemRec> TLevelIndex;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::THullChange<TKey, TMemRec> THullChange;
        typedef ::NKikimr::THullCompaction<TKey, TMemRec, TIterator> TThis;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;
        typedef ::NKikimr::THullSegmentsLoaded<TKey, TMemRec> THullSegmentsLoaded;
        typedef ::NKikimr::TOrderedLevelSegmentsLoader<TKey, TMemRec> TOrderedLevelSegmentsLoader;
        typedef ::NKikimr::THandoffMap<TKey, TMemRec> THandoffMap;
        typedef TIntrusivePtr<THandoffMap> THandoffMapPtr;
        typedef ::NKikimr::TGcMap<TKey, TMemRec> TGcMap;
        typedef typename TGcMap::TIterator TGcMapIterator;
        typedef TIntrusivePtr<TGcMap> TGcMapPtr;
        typedef ::NKikimr::TFreshSegment<TKey, TMemRec> TFreshSegment;
        typedef ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec> TFreshSegmentSnapshot;
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
        typedef ::NKikimr::TLevelIndexRunTimeCtx<TKey, TMemRec> TLevelIndexRunTimeCtx;

        using THullCompactionWorker = NKikimr::THullCompactionWorker<TKey, TMemRec, TIterator>;

        friend class TActorBootstrapped<TThis>;

        THullCtxPtr HullCtx;
        TPDiskCtxPtr PDiskCtx;
        const TActorId LIActor;
        // FreshSegment to compact if any
        TIntrusivePtr<TFreshSegment> FreshSegment;
        std::shared_ptr<TFreshSegmentSnapshot> FreshSegmentSnap;
        TBarriersSnapshot BarriersSnap;
        TLevelIndexSnapshot LevelSnap;
        TActiveActors ActiveActors;

        THandoffMapPtr Hmp;
        TGcMapPtr Gcmp;
        TIterator It;

        THullCompactionWorker Worker;
        const ui64 CompactionID;
        TOrderedLevelSegmentsPtr Result;

        // messages we have to send to Yard
        TVector<std::unique_ptr<IEventBase>> MsgsForYard;

        TActorId SkeletonId;

        bool IsAborting = false;
        ui32 PendingResponses = 0;

        ///////////////////////// BOOTSTRAP ////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            Worker.Statistics.StartTime = TAppData::TimeProvider->Now();

            LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                       VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                             "%s: Compaction job (%" PRIu64 ") started: fresh# %s freedHugeBlobs# %s",
                             PDiskSignatureForHullDbKey<TKey>().ToString().data(), CompactionID,
                             (FreshSegment ? "true" : "false"), Worker.GetFreedHugeBlobs().ToString().data()));

            // bool debug output of brs
            int brsDebugLevel = 0;
            // debug output of brs
            {
                ::NActors::NLog::TSettings *mSettings = (::NActors::NLog::TSettings*)((ctx).LoggerSettings());
                ::NActors::NLog::EPriority mPriority = ::NActors::NLog::PRI_INFO;
                ::NActors::NLog::EComponent mComponent = (::NActors::NLog::EComponent)( NKikimrServices::BS_HULLCOMP);

                bool output = mSettings && mSettings->Satisfies(mPriority, mComponent, 0);
                brsDebugLevel = output ? 1 : 0;
            }

            // build barriers essence
            auto brs = BarriersSnap.CreateEssence(HullCtx, 0, Max<ui64>(), brsDebugLevel);

            // free barriers snapshot
            BarriersSnap.Destroy();

            // build handoff map (use LevelSnap by ref)
            Hmp->BuildMap(LevelSnap, It);

            // build gc map (use LevelSnap by ref)
            Gcmp->BuildMap(ctx, brs, LevelSnap, It);
            TGcMapIterator gcmpIt = TGcMapIterator(Gcmp.Get());

            // free level snapshot
            LevelSnap.Destroy();

            // enter work state, prepare, and kick worker class
            TThis::Become(&TThis::WorkFunc);
            Worker.Prepare(Hmp, gcmpIt);
            MainCycle(ctx);
        }

        ///////////////////////// WORK: BEGIN ///////////////////////////////////////////////
        void MainCycle(const TActorContext& ctx) {
            // we invoke worker main cycle that possibly generates events for PDisk, they are stored in MsgsForYard; if
            // there are events, we send them to yard; worker internally controls all in flight limits and does not
            // generate more events than allowed; this function returns boolean status indicating whether compaction job
            // is finished or not
            const bool done = Worker.MainCycle(MsgsForYard);
            // check if there are messages we have for yard
            for (std::unique_ptr<IEventBase>& msg : MsgsForYard) {
                ctx.Send(PDiskCtx->PDiskId, msg.release());
                ++PendingResponses;
            }
            MsgsForYard.clear();
            // when done, continue with other state
            if (done) {
                Finalize(ctx);
            }
        }

        bool FinalizeIfAborting(const TActorContext& ctx) {
            if (IsAborting) {
                if (!PendingResponses) {
                    Finalize(ctx);
                }
                return true;
            } else {
                return false;
            }
        }

        // the same logic for every yard response: apply response and restart main cycle
        void HandleYardResponse(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext &ctx) {
            --PendingResponses;
            if (HullCtx->VCtx->CostTracker) {
                HullCtx->VCtx->CostTracker->CountPDiskResponse();
            }
            if (ev->Get()->Status != NKikimrProto::CORRUPTED) {
                CHECK_PDISK_RESPONSE(HullCtx->VCtx, ev, ctx);
            }
            if (FinalizeIfAborting(ctx)) {
                return;
            }
            TEvRestoreCorruptedBlob *msg = Worker.Apply(ev->Get(), ctx.Now());
            MainCycle(ctx);
            if (msg) {
                ctx.Send(SkeletonId, msg);
                ++PendingResponses;
            }
        }

        void Handle(TEvRestoreCorruptedBlobResult::TPtr& ev, const TActorContext& ctx) {
            --PendingResponses;
            if (FinalizeIfAborting(ctx)) {
                return;
            }
            TEvRestoreCorruptedBlob *msg = Worker.Apply(ev->Get(), &IsAborting, ctx.Now());
            if (FinalizeIfAborting(ctx)) {
                return;
            }
            MainCycle(ctx);
            if (msg) {
                ctx.Send(SkeletonId, msg);
                ++PendingResponses;
            }
        }

        void HandleYardResponse(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext &ctx) {
            --PendingResponses;
            if (HullCtx->VCtx->CostTracker) {
                HullCtx->VCtx->CostTracker->CountPDiskResponse();
            }
            CHECK_PDISK_RESPONSE(HullCtx->VCtx, ev, ctx);
            if (FinalizeIfAborting(ctx)) {
                return;
            }
            Worker.Apply(ev->Get());
            MainCycle(ctx);
        }

        void HandleYardResponse(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
            --PendingResponses;
            CHECK_PDISK_RESPONSE(HullCtx->VCtx, ev, ctx);
            if (FinalizeIfAborting(ctx)) {
                return;
            }

            LOG_INFO(ctx, NKikimrServices::BS_SKELETON,
                    VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "comp reserve ChunkIds# %s", FormatList(ev->Get()->ChunkIds).data()));

            Worker.Apply(ev->Get());
            MainCycle(ctx);
        }

        STRICT_STFUNC(WorkFunc,
            HFunc(NPDisk::TEvChunkReserveResult, HandleYardResponse)
            HFunc(NPDisk::TEvChunkWriteResult, HandleYardResponse)
            HFunc(NPDisk::TEvChunkReadResult, HandleYardResponse)
            HFunc(TEvRestoreCorruptedBlobResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )
        ///////////////////////// WORK: END /////////////////////////////////////////////////


        ///////////////////////// FINALIZE: BEGIN ///////////////////////////////////////////
        void Finalize(const TActorContext &ctx) {
            if (const auto& segs = Worker.GetLevelSegments(); segs && !IsAborting) {
                Result = MakeIntrusive<TOrderedLevelSegments>(segs.begin(), segs.end());
                Finish(ctx, false);
            } else {
                Finish(ctx, true);
            }
        }
        ///////////////////////// LOAD: END /////////////////////////////////////////////////


        ///////////////////////// FINISH ACTIVITY: BEGIN ////////////////////////////////////
        void Finish(const TActorContext &ctx, bool emptyWrite) {
            // prepare commit message
            std::unique_ptr<THullChange> msg(new THullChange());
            const auto& reservedChunks = IsAborting ? Worker.GetAllocatedChunks() : Worker.GetReservedChunks();
            msg->ReservedChunks = {reservedChunks.begin(), reservedChunks.end()};

            // huge blobs to free
            LOG_LOG(ctx, IsAborting ? NLog::PRI_ERROR : NLog::PRI_INFO, NKikimrServices::BS_HULLCOMP,
                       VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "%s: Compaction job (%" PRIu64 ") finished (freedHugeBlobs): fresh# %s freedHugeBlobs# %s",
                            PDiskSignatureForHullDbKey<TKey>().ToString().data(), CompactionID,
                            (FreshSegment ? "true" : "false"), Worker.GetFreedHugeBlobs().ToString().data()));
            msg->FreedHugeBlobs = IsAborting ? TDiskPartVec() : Worker.GetFreedHugeBlobs();

            // chunks to commit
            msg->CommitChunks = IsAborting ? TVector<ui32>() : Worker.GetCommitChunks();

            Y_ABORT_UNLESS(emptyWrite == msg->CommitChunks.empty()); // both empty or not

            msg->SegVec = IsAborting ? nullptr : std::move(Result);
            msg->FreshSegment = IsAborting ? nullptr : FreshSegment;
            msg->Aborted = IsAborting;

            Worker.Statistics.FinishTime = TAppData::TimeProvider->Now();
            LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                       VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                             "%s: Compaction job (%" PRIu64 ") finished: fresh# %s chunks# %" PRIu32 " stat# %s "
                             "gcmpStat# %s IsAborting# %s",
                             PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                             CompactionID, (FreshSegment ? "true" : "false"), ui32(msg->CommitChunks.size()),
                             Worker.Statistics.ToString().data(), Gcmp->GetStat().ToString().data(),
                             IsAborting ? "true" : "false"));

            ctx.Send(LIActor, msg.release());
            TThis::Die(ctx);
        }
        ///////////////////////// FINISH ACTIVITY: END //////////////////////////////////////

        PDISK_TERMINATE_STATE_FUNC_DEF;

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::VDISK_COMPACTION;
        }

        THullCompaction(THullCtxPtr hullCtx,
                        const std::shared_ptr<TLevelIndexRunTimeCtx> &rtCtx,
                        TIntrusivePtr<TFreshSegment> freshSegment,
                        std::shared_ptr<TFreshSegmentSnapshot> freshSegmentSnap,
                        TBarriersSnapshot &&barriersSnap,
                        TLevelIndexSnapshot &&levelSnap,
                        ui64 mergeElementsApproximation,
                        const TIterator &it,
                        ui64 firstLsn,
                        ui64 lastLsn,
                        TDuration restoreDeadline,
                        std::optional<TKey> partitionKey,
                        bool allowGarbageCollection)
            : TActorBootstrapped<TThis>()
            , HullCtx(std::move(hullCtx))
            , PDiskCtx(rtCtx->PDiskCtx)
            , LIActor(rtCtx->LevelIndex->LIActor)
            , FreshSegment(std::move(freshSegment))
            , FreshSegmentSnap(std::move(freshSegmentSnap))
            , BarriersSnap(std::move(barriersSnap))
            , LevelSnap(std::move(levelSnap))
            , Hmp(CreateHandoffMap<TKey, TMemRec>(HullCtx, rtCtx->RunHandoff, rtCtx->SkeletonId))
            , Gcmp(CreateGcMap<TKey, TMemRec>(HullCtx, mergeElementsApproximation, allowGarbageCollection))
            , It(it)
            , Worker(HullCtx, PDiskCtx, rtCtx->LevelIndex, it, (bool)FreshSegment, firstLsn, lastLsn, restoreDeadline,
                    partitionKey)
            , CompactionID(TAppData::RandomProvider->GenRand64())
            , SkeletonId(rtCtx->SkeletonId)
        {}
    };

} // NKikimr
