#pragma once

#include "defs.h"
#include "blobstorage_hullcompactworker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events_quoter.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/queue.h>
#include <optional>


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
        TDiskPartVec AllocatedHugeBlobs;
        TDiskPartVec AllocatedStripeBlobs;
        // was the compaction process aborted by some reason?
        bool Aborted = false;
        bool FreshCompaction = false;

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
        std::optional<TBarriersSnapshot> BarriersSnap;
        TLevelIndexSnapshot LevelSnap;
        TActiveActors ActiveActors;

        THandoffMapPtr Hmp;
        TIterator It;

        THullCompactionWorker Worker;
        const ui64 CompactionID;

        // Planned compaction (EnableVDiskPlannedCompaction): a worker of its own goes through the job first, writing
        // nothing, to find out exactly how many chunks it needs; those are reserved in one go before anything is
        // written, and the job writes into them only.
        enum {
            EvPlanQuantum = EventSpaceBegin(TEvents::ES_PRIVATE),
        };
        struct TEvPlanQuantum : TEventLocal<TEvPlanQuantum, EvPlanQuantum> {};
        static constexpr ui32 PlanQuantumItems = 10000;
        std::optional<THullCompactionWorker> Planner;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> Barriers; // while planning
        ui32 PlannedChunks = 0;
        TOrderedLevelSegmentsPtr Result;

        // messages we have to send to Yard
        TVector<std::unique_ptr<IEventBase>> MsgsForYard;

        const TActorId SkeletonId;
        const TActorId HugeKeeperId;

        bool IsAborting = false;
        ui32 PendingResponses = 0;

        //  Compaction throttler
        TEventsQuoter::TPtr Throttler;

        ///////////////////////// BOOTSTRAP ////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            Worker.Statistics.StartTime = TAppData::TimeProvider->Now();

            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::BS_HULLCOMP, VDISKP(HullCtx->VCtx->VDiskLogPrefix, "%s: Compaction job (%" PRIu64 ") started: fresh# %s freedHugeBlobs# %s", PDiskSignatureForHullDbKey<TKey>().ToString().data(), CompactionID, (FreshSegment ? "true" : "false"), Worker.GetFreedHugeBlobs().ToString().data()));

            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> brs;
            if constexpr (!std::is_same_v<TKey, TKeyBlock>) {
                // Blocks compaction does not consult barriers: records are merged, never dropped
                // (except that a Max generation block later lets us drop barriers of that tablet).
                Y_VERIFY_S(BarriersSnap, HullCtx->VCtx->VDiskLogPrefix);

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
                brs = BarriersSnap->CreateEssence(HullCtx, 0, Max<ui64>(), brsDebugLevel);

                // free barriers snapshot
                BarriersSnap->Destroy();
                BarriersSnap.reset();
            }

            // build handoff map (use LevelSnap by ref)
            Hmp->BuildMap(LevelSnap, It);

            if (Planner) {
                Barriers = std::move(brs);
                Planner->Prepare(Hmp, Barriers, &LevelSnap);
                TThis::Become(&TThis::PlanFunc);
                ctx.Send(ctx.SelfID, new TEvPlanQuantum);
            } else {
                StartWork(ctx, std::move(brs));
            }
        }

        // enter work state, prepare, and kick worker class
        void StartWork(const TActorContext& ctx, TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> brs) {
            TThis::Become(&TThis::WorkFunc);
            Worker.Prepare(Hmp, std::move(brs), &LevelSnap);
            MainCycle(ctx);
        }

        ///////////////////////// PLAN: BEGIN ///////////////////////////////////////////////
        void Plan(const TActorContext& ctx) {
            if (!Planner->PlanQuantum(PlanQuantumItems)) {
                ctx.Send(ctx.SelfID, new TEvPlanQuantum); // let the mailbox breathe
                return;
            }
            PlannedChunks = Planner->GetPlannedChunks();
            Planner.reset();
            Hmp->RestartTransform();

            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::BS_HULLCOMP, "Compaction job planned",
                {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                {"signature", PDiskSignatureForHullDbKey<TKey>()},
                {"compactionID", CompactionID},
                {"plannedChunks", PlannedChunks});

            if (PlannedChunks) {
                ctx.Send(PDiskCtx->PDiskId, new NPDisk::TEvChunkReserve(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound,
                    PlannedChunks, /*forHousekeeping=*/true));
            } else {
                StartWork(ctx, std::move(Barriers)); // it writes nothing
            }
        }

        void HandlePlanReserve(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
            if (ev->Get()->Status == NKikimrProto::OUT_OF_SPACE) {
                // Nothing has been written; the arbiter learns from the release that it did not fit after all.
                YDB_LOG_NOTICE_CTX_COMP(ctx, NKikimrServices::BS_HULLCOMP, "Planned compaction does not fit",
                    {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                    {"compactionID", CompactionID},
                    {"plannedChunks", PlannedChunks},
                    {"errorReason", ev->Get()->ErrorReason});
                IsAborting = true;
                Finish(ctx, true);
                return;
            }
            CHECK_PDISK_RESPONSE(HullCtx->VCtx, ev, ctx);
            Y_VERIFY_S(ev->Get()->ChunkIds.size() == PlannedChunks, HullCtx->VCtx->VDiskLogPrefix);
            Worker.AddPreReservedChunks(ev->Get()->ChunkIds);
            StartWork(ctx, std::move(Barriers));
        }

        STRICT_STFUNC(PlanFunc,
            CFunc(EvPlanQuantum, Plan)
            HFunc(NPDisk::TEvChunkReserveResult, HandlePlanReserve)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )
        ///////////////////////// PLAN: END /////////////////////////////////////////////////

        ///////////////////////// WORK: BEGIN ///////////////////////////////////////////////
        void MainCycle(const TActorContext& ctx) {
            // we invoke worker main cycle that possibly generates events for PDisk, they are stored in MsgsForYard; if
            // there are events, we send them to yard; worker internally controls all in flight limits and does not
            // generate more events than allowed; this function returns boolean status indicating whether compaction job
            // is finished or not
            std::vector<ui32> *slotsToAllocate = nullptr;
            const bool done = Worker.MainCycle(MsgsForYard, &slotsToAllocate);
            // check if there are messages we have for yard
            for (std::unique_ptr<IEventBase>& msg : MsgsForYard) {
                ui64 bytes = GetMsgSize(msg);
                TEventsQuoter::QuoteMessage(Throttler, std::make_unique<IEventHandle>(
                            PDiskCtx->PDiskId, ctx.SelfID, msg.release()), bytes, HullCtx->VCfg->HullCompThrottlerBytesRate);
                ++PendingResponses;
            }

            MsgsForYard.clear();
            // send slots to allocate to huge keeper, if any
            if (slotsToAllocate) {
                ctx.Send(HugeKeeperId, new TEvHugeAllocateSlots(std::move(*slotsToAllocate)));
            }
            if (Worker.IsPlanExceeded() && !IsAborting) {
                YDB_LOG_CRIT_CTX_COMP(ctx, NKikimrServices::BS_HULLCOMP, "Planned compaction needs more chunks than planned",
                    {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                    {"compactionID", CompactionID},
                    {"plannedChunks", PlannedChunks},
                    {"marker", "BSHC51"});
                Y_DEBUG_ABORT("planned compaction needs more chunks than planned");
                IsAborting = true;
                FinalizeIfAborting(ctx);
                return;
            }
            // when done, continue with other state
            if (done) {
                Finalize(ctx);
            }
        }

        ui32 GetMsgSize(std::unique_ptr<IEventBase>& msg) {
            if (msg->Type() == TEvBlobStorage::EvChunkWrite) {
                auto *write = static_cast<NPDisk::TEvChunkWrite*>(msg.get());
                return write->PartsPtr ? write->PartsPtr->ByteSize() : 0;
            } else if (msg->Type() == TEvBlobStorage::EvChunkRead) {
                auto *read = static_cast<NPDisk::TEvChunkRead*>(msg.get());
                return read->Size;
            }
            return 0;
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
            if (ev->Get()->Status == NKikimrProto::OUT_OF_SPACE) {
                IsAborting = true;
                const bool flag = FinalizeIfAborting(ctx);
                Y_ABORT_UNLESS(flag);
                return;
            }
            CHECK_PDISK_RESPONSE(HullCtx->VCtx, ev, ctx);
            if (FinalizeIfAborting(ctx)) {
                return;
            }

            YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::BS_SKELETON, VDISKP(HullCtx->VCtx->VDiskLogPrefix, "comp reserve ChunkIds# %s", FormatList(ev->Get()->ChunkIds).data()));

            Worker.Apply(ev->Get());
            MainCycle(ctx);
        }

        void Handle(TEvHugeAllocateSlotsResult::TPtr ev, const TActorContext& ctx) {
            Worker.Apply(ev->Get());
            MainCycle(ctx);
        }

        STRICT_STFUNC(WorkFunc,
            HFunc(NPDisk::TEvChunkReserveResult, HandleYardResponse)
            HFunc(NPDisk::TEvChunkWriteResult, HandleYardResponse)
            HFunc(NPDisk::TEvChunkReadResult, HandleYardResponse)
            HFunc(TEvRestoreCorruptedBlobResult, Handle)
            HFunc(TEvHugeAllocateSlotsResult, Handle)
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

            YDB_LOG_CTX_COMP(ctx, IsAborting ? NLog::PRI_ERROR : NLog::PRI_INFO, NKikimrServices::BS_HULLCOMP, "Compaction job finished",
                {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                {"signature", PDiskSignatureForHullDbKey<TKey>()},
                {"compactionID", CompactionID},
                {"freshSegment", (FreshSegment ? "true" : "false")},
                {"freedHugeBlobs", Worker.GetFreedHugeBlobs().Size()},
                {"allocatedHugeBlobs", Worker.GetAllocatedHugeBlobs().Size()},
                {"commitChunks", FormatList(Worker.GetCommitChunks())},
                {"stats", Worker.Statistics},
                {"isAborting", (IsAborting ? "true" : "false")});

            msg->FreedHugeBlobs = IsAborting ? TDiskPartVec() : Worker.GetFreedHugeBlobs();
            msg->AllocatedHugeBlobs = IsAborting ? TDiskPartVec() : Worker.GetAllocatedHugeBlobs();
            msg->AllocatedStripeBlobs = IsAborting ? TDiskPartVec() : Worker.GetAllocatedStripeBlobs();

            if (IsAborting) { // release previously preallocated slots for huge blobs if we are aborting
                std::vector<TDiskPart> drop = Worker.GetAllocatedHugeBlobs().Vec;
                drop.insert(drop.end(), Worker.GetAllocatedStripeBlobs().Vec.begin(),
                    Worker.GetAllocatedStripeBlobs().Vec.end());
                ctx.Send(HugeKeeperId, new TEvHugeDropAllocatedSlots(std::move(drop)));
            }

            // chunks to commit
            msg->CommitChunks = IsAborting ? TVector<ui32>() : Worker.GetCommitChunks();

            Y_VERIFY_S(emptyWrite == (msg->CommitChunks.empty() && msg->AllocatedStripeBlobs.Empty()),
                HullCtx->VCtx->VDiskLogPrefix); // both empty or not

            msg->SegVec = IsAborting ? nullptr : std::move(Result);
            msg->FreshSegment = IsAborting ? nullptr : FreshSegment;
            msg->Aborted = IsAborting;
            msg->FreshCompaction = static_cast<bool>(FreshSegment);

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
                        THugeBlobCtxPtr hugeBlobCtx,
                        ui32 minHugeBlobInBytes,
                        TIntrusivePtr<TFreshSegment> freshSegment,
                        std::shared_ptr<TFreshSegmentSnapshot> freshSegmentSnap,
                        std::optional<TBarriersSnapshot> &&barriersSnap,
                        TLevelIndexSnapshot &&levelSnap,
                        const TIterator &it,
                        ui64 firstLsn,
                        ui64 lastLsn,
                        TDuration restoreDeadline,
                        std::optional<TKey> partitionKey,
                        bool allowGarbageCollection,
                        bool useThrottle,
                        bool planned = false)
            : TActorBootstrapped<TThis>()
            , HullCtx(std::move(hullCtx))
            , PDiskCtx(rtCtx->PDiskCtx)
            , LIActor(rtCtx->LevelIndex->LIActor)
            , FreshSegment(std::move(freshSegment))
            , FreshSegmentSnap(std::move(freshSegmentSnap))
            , BarriersSnap(std::move(barriersSnap))
            , LevelSnap(std::move(levelSnap))
            , Hmp(CreateHandoffMap<TKey, TMemRec>(HullCtx, rtCtx->RunHandoff, rtCtx->SkeletonId))
            , It(it)
            , Worker(HullCtx, PDiskCtx, hugeBlobCtx, minHugeBlobInBytes, rtCtx->LevelIndex, it,
                static_cast<bool>(FreshSegment), firstLsn, lastLsn, restoreDeadline, partitionKey, allowGarbageCollection)
            , CompactionID(TAppData::RandomProvider->GenRand64())
            , SkeletonId(rtCtx->SkeletonId)
            , HugeKeeperId(rtCtx->HugeKeeperId)
        {
            if (!(bool)FreshSegment && useThrottle) {
                Throttler = std::make_shared<TEventsQuoter>();
            }
            if (planned) {
                Worker.SetPlanned();
                Planner.emplace(HullCtx, PDiskCtx, std::move(hugeBlobCtx), minHugeBlobInBytes, rtCtx->LevelIndex, it,
                    false, firstLsn, lastLsn, restoreDeadline, partitionKey, allowGarbageCollection);
                Planner->SetPlanned();
            }
        }

        // Before the actor starts: chunks the Fresh segment reserved for this compaction in advance.
        void AddPreReservedChunks(const TVector<TChunkIdx>& chunks) {
            Worker.AddPreReservedChunks(chunks);
        }
    };

} // NKikimr
