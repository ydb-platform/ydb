#include "blobstorage_hullactor.h"
#include "blobstorage_hullcommit.h"
#include "blobstorage_hullcompact.h"
#include "blobstorage_buildslice.h"
#include "hullop_compactfreshappendix.h"
#include <ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_selector.h>
#include <ydb/core/blobstorage/vdisk/hullop/hullcompdelete/blobstorage_hullcompdelete.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TFullCompactionState
    ////////////////////////////////////////////////////////////////////////////
    struct TFullCompactionState {
        struct TCompactionRequest {
            EHullDbType Type = EHullDbType::Max;
            ui64 RequestId = 0;
            TActorId Recipient;
        };
        std::deque<TCompactionRequest> Requests;
        std::optional<NHullComp::TFullCompactionAttrs> FullCompactionAttrs;

        bool Enabled() const {
            return bool(FullCompactionAttrs);
        }

        void FullCompactionTask(ui64 fullCompactionLsn, TInstant now, EHullDbType type, ui64 requestId,
                const TActorId &recipient, THashSet<ui64> tablesToCompact)
        {
            FullCompactionAttrs.emplace(fullCompactionLsn, now, std::move(tablesToCompact));
            Requests.push_back({type, requestId, recipient});
        }

        void Compacted(const TActorContext& ctx, const std::pair<std::optional<NHullComp::TFullCompactionAttrs>, bool>& info) {
            if (Enabled() && FullCompactionAttrs == info.first && info.second) {
                // full compaction finished
                for (const auto &x : Requests) {
                    ctx.Send(x.Recipient, new TEvHullCompactResult(x.Type, x.RequestId));
                }
                Requests.clear();
                FullCompactionAttrs.reset();
            }
        }

        // returns FullCompactionAttrs for Level Compaction Selector
        // if Fresh segment before FullCompactionAttrs->FullCompationLsn has not been written to sst yet,
        // there is no profit in starting LevelCompaction, so we return nullopt
        template <class TRTCtx>
        std::optional<NHullComp::TFullCompactionAttrs> GetFullCompactionAttrsForLevelCompactionSelector(const TRTCtx &rtCtx) {
            return Enabled() && rtCtx->LevelIndex->IsWrittenToSstBeforeLsn(FullCompactionAttrs->FullCompactionLsn)
                ? FullCompactionAttrs
                : std::nullopt;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // FRESH compaction
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    void CompactFreshSegment(
            TIntrusivePtr<THullDs> &hullDs,
            THugeBlobCtxPtr hugeBlobCtx,
            ui32 minHugeBlobInBytes,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKey, TMemRec>> &rtCtx,
            const TActorContext &ctx,
            bool allowGarbageCollection)
    {
        using TFreshSegment = ::NKikimr::TFreshSegment<TKey, TMemRec>;
        using TFreshSegmentSnapshot = ::NKikimr::TFreshSegmentSnapshot<TKey, TMemRec>;
        using TIterator = typename TFreshSegmentSnapshot::TForwardIterator;
        using TFreshCompaction = ::NKikimr::THullCompaction<TKey, TMemRec, TIterator>;

        auto &hullCtx = hullDs->HullCtx;
        Y_VERIFY_S(hullCtx->FreshCompaction, hullCtx->VCtx->VDiskLogPrefix);

        // get fresh segment to compact
        TIntrusivePtr<TFreshSegment> freshSegment = rtCtx->LevelIndex->FindFreshSegmentForCompaction();
        Y_VERIFY_S(freshSegment, hullCtx->VCtx->VDiskLogPrefix);

        // prepare snapshots
        auto barriersSnap = hullDs->Barriers->GetIndexSnapshot();
        auto levelSnap = rtCtx->LevelIndex->GetIndexSnapshot();

        // prepare iterator and first/last lsns
        auto freshSegmentSnap = std::make_shared<TFreshSegmentSnapshot>(freshSegment->GetSnapshot());
        TIterator it(hullCtx, freshSegmentSnap.get());
        it.SeekToFirst();
        ui64 firstLsn = freshSegment->GetFirstLsn();
        ui64 lastLsn = freshSegment->GetLastLsn();
        std::unique_ptr<TFreshCompaction> compaction(new TFreshCompaction(
            hullCtx, rtCtx, std::move(hugeBlobCtx), minHugeBlobInBytes, freshSegment, freshSegmentSnap,
            std::move(barriersSnap), std::move(levelSnap), it, firstLsn, lastLsn, TDuration::Max(), {},
            allowGarbageCollection));

        LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                VDISKP(hullCtx->VCtx->VDiskLogPrefix,
                    "%s: fresh scheduled", PDiskSignatureForHullDbKey<TKey>().ToString().data()));

        Y_VERIFY_S(lastLsn <= rtCtx->LsnMngr->GetConfirmedLsnForHull(), hullCtx->VCtx->VDiskLogPrefix
                << "Last fresh lsn MUST be confirmed; lastLsn# " << lastLsn
                << " confirmed# " << rtCtx->LsnMngr->GetConfirmedLsnForHull());

        auto actorId = RunInBatchPool(ctx, compaction.release());
        rtCtx->LevelIndex->ActorCtx->ActiveActors.Insert(actorId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
    }

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexActor. We run it on the same mailbox as Skeleton,
    // it is used for commits and compaction scheduling
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexActor : public TActorBootstrapped<TLevelIndexActor<TKey, TMemRec>> {
        typedef ::NKikimr::THullChange<TKey, TMemRec> THullChange;
        typedef ::NKikimr::TFreshAppendixCompactionDone<TKey, TMemRec> TFreshAppendixCompactionDone;
        typedef ::NKikimr::TLevelIndexActor<TKey, TMemRec> TThis;
        typedef ::NKikimr::NHullComp::TTask<TKey, TMemRec> TCompactionTask;
        typedef ::NKikimr::NHullComp::TSelectorActor<TKey, TMemRec> TSelectorActor;
        typedef ::NKikimr::NHullComp::TSelected<TKey, TMemRec> TSelected;

        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;
        typedef TIntrusivePtr<TLevelSlice> TLevelSlicePtr;
        typedef typename TLevelSlice::TForwardIterator TLevelSliceForwardIterator;
        typedef ::NKikimr::THullCompaction<TKey, TMemRec, TLevelSliceForwardIterator> TLevelCompaction;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::TLeveledSsts<TKey, TMemRec> TLeveledSsts;
        typedef typename TLeveledSsts::TIterator TLeveledSstsIterator;
        typedef ::NKikimr::TAsyncLevelCommitter<TKey, TMemRec> TAsyncLevelCommitter;
        typedef ::NKikimr::TAsyncFreshCommitter<TKey, TMemRec> TAsyncFreshCommitter;
        typedef ::NKikimr::TAsyncAdvanceLsnCommitter<TKey, TMemRec> TAsyncAdvanceLsnCommitter;
        typedef ::NKikimr::TAsyncReplSstCommitter<TKey, TMemRec> TAsyncReplSstCommitter;

        using TRunTimeCtx = TLevelIndexRunTimeCtx<TKey, TMemRec>;
        using THullOpUtil = ::NKikimr::THullOpUtil<TKey, TMemRec>;

        //
        // StateNoComp -> StateCompPolicyAtWork -> StateCompInProgress -> StateWaitCommit -+
        //      ^  ^                |    |                                       ^         |
        //      |  |                |    |                                       |         |
        //      |  +----------------+    +---------------------------------------+         |
        //      +--------------------------------------------------------------------------+

        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<THullDs> HullDs;
        std::shared_ptr<THullLogCtx> HullLogCtx;
        std::shared_ptr<TRunTimeCtx> RTCtx;
        std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> SyncLogFirstLsnToKeep;
        NHullComp::TBoundariesConstPtr Boundaries;
        THullDbCommitterCtxPtr HullDbCommitterCtx;
        std::unique_ptr<TCompactionTask> CompactionTask;
        bool AdvanceCommitInProgress = false;
        TActiveActors &ActiveActors;
        NMonGroup::TLsmAllLevelsStat LevelStat;
        TFullCompactionState FullCompactionState;
        bool CompactionScheduled = false;
        TMonotonic NextCompactionWakeup;
        bool AllowGarbageCollection = false;
        THugeBlobCtxPtr HugeBlobCtx;
        ui32 MinHugeBlobInBytes;

        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            RTCtx->LevelIndex->UpdateLevelStat(LevelStat);
            ScheduleCompaction(ctx);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // RunLevelCompactionSelector runs TSelectorActor which selects what to compact.
        // returns true, if selector has been started, false otherwise
        bool RunLevelCompactionSelector(const TActorContext &ctx) {
            // if compaction is in progress or disabled, return
            if (RTCtx->LevelIndex->GetCompState() != TLevelIndexBase::StateNoComp || !Config->LevelCompaction ||
                    Config->BaseInfo.DonorMode) {
                return false;
            }

            //////////////////////// CHOOSE WHAT TO COMPACT ///////////////////////////////
            RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateCompPolicyAtWork);
            auto barriersSnap = HullDs->Barriers->GetIndexSnapshot();
            auto levelSnap = RTCtx->LevelIndex->GetIndexSnapshot();
            const double rateThreshold = Config->HullCompLevelRateThreshold;
            auto fullCompactionAttrs = FullCompactionState.GetFullCompactionAttrsForLevelCompactionSelector(RTCtx);
            NHullComp::TSelectorParams params = {Boundaries, rateThreshold, TInstant::Seconds(0), fullCompactionAttrs};
            auto selector = std::make_unique<TSelectorActor>(HullDs->HullCtx, params, std::move(levelSnap),
                std::move(barriersSnap), ctx.SelfID, std::move(CompactionTask), AllowGarbageCollection);
            auto aid = RunInBatchPool(ctx, selector.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            return true;
        }

        void ScheduleCompactionWakeup(const TActorContext& ctx) {
            NextCompactionWakeup = ctx.Monotonic() + Config->HullCompSchedulingInterval;
            if (!CompactionScheduled) {
                ctx.Schedule(NextCompactionWakeup, new TEvents::TEvWakeup);
                CompactionScheduled = true;
            }
        }

        void HandleWakeup(const TActorContext& ctx) {
            Y_VERIFY_S(CompactionScheduled, HullDs->HullCtx->VCtx->VDiskLogPrefix);
            CompactionScheduled = false;
            if (ctx.Monotonic() >= NextCompactionWakeup) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, "Try to schedule compactions");
                ScheduleCompaction(ctx);
            } else {
                ScheduleCompactionWakeup(ctx);
            }
        }

        bool ScheduleCompaction(const TActorContext &ctx, bool level = true) {
            // schedule fresh if required
            const bool res = CompactFreshSegmentIfRequired<TKey, TMemRec>(HullDs, HugeBlobCtx, MinHugeBlobInBytes, RTCtx,
                ctx, !RTCtx->LevelIndex->IsWrittenToSstBeforeLsn(ForceFreshCompactLsn), AllowGarbageCollection);
            if (level && !Config->BaseInfo.ReadOnly && !RunLevelCompactionSelector(ctx)) {
                ScheduleCompactionWakeup(ctx);
            }
            return res;
        }

        void RunLevelCompaction(const TActorContext &ctx, TVector<TOrderedLevelSegmentsPtr> &vec) {
            RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateCompInProgress);

            // set up lsns + find out number of elements to merge
            ui64 firstLsn = ui64(-1);
            ui64 lastLsn = 0;
            for (const auto &seg : vec) {
                firstLsn = Min(firstLsn, seg->GetFirstLsn());
                lastLsn = Max(lastLsn, seg->GetLastLsn());
            }

            // prepare snapshots
            auto barriersSnap = HullDs->Barriers->GetIndexSnapshot();
            auto levelSnap = RTCtx->LevelIndex->GetIndexSnapshot();
            // set up iterator
            TLevelSliceForwardIterator it(HullDs->HullCtx, vec);
            it.SeekToFirst();

            std::unique_ptr<TLevelCompaction> compaction(new TLevelCompaction(HullDs->HullCtx, RTCtx, HugeBlobCtx,
                MinHugeBlobInBytes, nullptr, nullptr, std::move(barriersSnap), std::move(levelSnap),
                it, firstLsn, lastLsn, TDuration::Minutes(2), {}, AllowGarbageCollection));
            NActors::TActorId actorId = RunInBatchPool(ctx, compaction.release());
            ActiveActors.Insert(actorId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(typename TSelected::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Y_VERIFY_S(RTCtx->LevelIndex->GetCompState() == TLevelIndexBase::StateCompPolicyAtWork,
                HullDs->HullCtx->VCtx->VDiskLogPrefix);
            RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateNoComp);

            NHullComp::EAction action = ev->Get()->Action;
            CompactionTask = std::move(ev->Get()->CompactionTask);

            LOG_LOG(ctx, action != NHullComp::ActNothing ? NLog::PRI_INFO : NLog::PRI_DEBUG,
                NKikimrServices::BS_HULLCOMP, VDISKP(HullDs->HullCtx->VCtx, "%s: selected compaction %s",
                PDiskSignatureForHullDbKey<TKey>().ToString().data(), CompactionTask->ToString().data()));

            switch (action) {
                case NHullComp::ActNothing: {
                    // notify compaction completed
                    FullCompactionState.Compacted(ctx, CompactionTask->FullCompactionInfo);
                    // nothing to merge, try later
                    ScheduleCompactionWakeup(ctx);
                    // for now, update storage ratio as it may have changed
                    UpdateStorageRatio(RTCtx->LevelIndex->CurSlice);
                    break;
                }
                case NHullComp::ActDeleteSsts: {
                    Y_VERIFY_S(CompactionTask->GetSstsToAdd().Empty() && !CompactionTask->GetSstsToDelete().Empty(),
                        HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    if (CompactionTask->GetHugeBlobsToDelete().Empty()) {
                        ApplyCompactionResult(ctx, {}, {}, 0);
                    } else {
                        // switch compaction state to pre-compaction to block any attempts of concurrent compaction
                        RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateWaitPreCompact);

                        const ui64 cookie = NextPreCompactCookie++;
                        LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, HullDs->HullCtx->VCtx->VDiskLogPrefix
                            << "requesting PreCompact for ActDeleteSsts");
                        ctx.Send(HullLogCtx->HugeKeeperId, new TEvHugePreCompact, 0, cookie);
                        PreCompactCallbacks.emplace(cookie, [this, ev](ui64 wId, const TActorContext& ctx) mutable {
                            Y_VERIFY_S(RTCtx->LevelIndex->GetCompState() == TLevelIndexBase::StateWaitPreCompact,
                                HullDs->HullCtx->VCtx->VDiskLogPrefix);
                            RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateNoComp);

                            Y_VERIFY_S(wId, HullDs->HullCtx->VCtx->VDiskLogPrefix);
                            LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, HullDs->HullCtx->VCtx->VDiskLogPrefix
                                << "got PreCompactResult for ActDeleteSsts, wId# " << wId);
                            ApplyCompactionResult(ctx, {}, {}, wId);
                            RTCtx->LevelIndex->UpdateLevelStat(LevelStat);
                        });
                        return;
                    }
                    break;
                }
                case NHullComp::ActMoveSsts: {
                    Y_VERIFY_S(!CompactionTask->GetSstsToAdd().Empty() && !CompactionTask->GetSstsToDelete().Empty(),
                        HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    ApplyCompactionResult(ctx, {}, {}, 0);
                    break;
                }
                case NHullComp::ActCompactSsts: {
                    // start compaction
                    LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                             VDISKP(HullDs->HullCtx->VCtx, "%s: level scheduled",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data()));
                    RunLevelCompaction(ctx, CompactionTask->CompactSsts.CompactionChains);
                    break;
                }
                default:
                    Y_ABORT("Unexpected case");
            }

            RTCtx->LevelIndex->UpdateLevelStat(LevelStat);
        }

        void CalculateStorageRatio(TLevelSlicePtr slice) {
            NHullComp::TSstRatio total;

            TLevelSliceSnapshot<TKey, TMemRec> sliceSnap(slice, slice->Level0CurSstsNum());
            typename TLevelSliceSnapshot<TKey, TMemRec>::TSstIterator it(&sliceSnap);
            it.SeekToFirst();
            while (it.Valid()) {
                if (NHullComp::TSstRatioPtr ratio = it.Get().SstPtr->StorageRatio.Get()) {
                    total += *ratio;
                }
                it.Next();
            }

            slice->LastPublishedRatio = total;
        }

        void UpdateStorageRatio(TLevelSlicePtr slice) {
            NHullComp::TSstRatio prev(slice->LastPublishedRatio);
            CalculateStorageRatio(slice);
            HullDs->HullCtx->UpdateSpaceCounters(prev, slice->LastPublishedRatio);
        }

        void CheckRemovedHugeBlobs(const TActorContext &ctx,
                                   const TDiskPartVec &calcVec,
                                   const TDiskPartVec &checkVec,
                                   bool level) const {
            if (Config->CheckHugeBlobs) {
                TVector<TDiskPart> v1 = calcVec.Vec;
                TVector<TDiskPart> v2 = checkVec.Vec;
                Sort(v1.begin(), v1.end());
                Sort(v2.begin(), v2.end());
                if (v1 != v2) {
                    LOG_CRIT(ctx, NKikimrServices::BS_HULLCOMP,
                             VDISKP(HullDs->HullCtx->VCtx, "HUGE BLOBS REMOVAL INCONSISTENCY: ctask# %s level# %s"
                                   " calcVec# %s checkVec# %s", CompactionTask->ToString().data(),
                                   (level ? "true" : "false"), calcVec.ToString().data(),
                                   checkVec.ToString().data()));
                }
            }
        }

        void ApplyCompactionResult(const TActorContext &ctx, TVector<ui32> chunksAdded, TVector<ui32> reservedChunksLeft,
                ui64 wId) {
            // create new slice
            RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateWaitCommit);

            // apply TCompactionTask (i.e. create a new slice)
            bool checkHugeBlobs = Config->CheckHugeBlobs;
            TLevelSlicePtr prevSlice = std::move(RTCtx->LevelIndex->CurSlice);
            typename THullOpUtil::TBuiltSlice cs = THullOpUtil::BuildSlice(HullDs->HullCtx->VCtx, ctx,
                    RTCtx->LevelIndex->Settings, prevSlice.Get(), *CompactionTask, checkHugeBlobs);
            RTCtx->LevelIndex->CurSlice = std::move(cs.NewSlice);
            // check huge blobs
            if (checkHugeBlobs) {
                CheckRemovedHugeBlobs(ctx, CompactionTask->GetHugeBlobsToDelete(), cs.RemovedHugeBlobs, true);
                LogRemovedHugeBlobs(ctx, CompactionTask->GetHugeBlobsToDelete(), true);
            }

            // delete list, includes previous ChunksToDelete and reserved chunks
            TVector<ui32> deleteChunks(std::move(prevSlice->ChunksToDelete));
            deleteChunks.insert(deleteChunks.end(), reservedChunksLeft.begin(), reservedChunksLeft.end());

            // only delete chunks if we actually delete SST's from yard; otherwise it is move operation, we delete them from one
            // level and put to another
            if (CompactionTask->CollectDeletedSsts()) {
                TLeveledSstsIterator delIt(&CompactionTask->GetSstsToDelete());
                for (delIt.SeekToFirst(); delIt.Valid(); delIt.Next()) {
                    const TLevelSegment& seg = *delIt.Get().SstPtr;
                    seg.FillInChunkIds(deleteChunks);
                    if (seg.Info.IsCreatedByRepl()) { // mark it out-of-index to schedule deletion from the bulk formed segments table
                        prevSlice->BulkFormedSegments.RemoveSstFromIndex(seg.GetEntryPoint());
                    }
                }
            }

            // transfer and update storage ratio to the new slice
            CalculateStorageRatio(RTCtx->LevelIndex->CurSlice);
            HullDs->HullCtx->UpdateSpaceCounters(prevSlice->LastPublishedRatio,
                RTCtx->LevelIndex->CurSlice->LastPublishedRatio);

            // apply compaction to bulk-formed SSTables; it produces a set of bulk-formed segments suitable for saving
            // in new slice containing only needed entries
            prevSlice->BulkFormedSegments.ApplyCompactionResult(RTCtx->LevelIndex->CurSlice->BulkFormedSegments, deleteChunks);

            // manage recovery log LSN to keep:
            // we can't advance LsnToKeep until the prev snapshot dies,
            // since we need to be able to read the rest of the log for remote recovery
            RTCtx->LevelIndex->PrevEntryPointLsn = RTCtx->LevelIndex->CurEntryPointLsn; // keep everything for prev snapshot

            // run level committer
            TDiskPartVec removedHugeBlobs(CompactionTask->ExtractHugeBlobsToDelete());
            TDiskPartVec allocatedHugeBlobs(CompactionTask->GetHugeBlobsAllocated());
            auto committer = std::make_unique<TAsyncLevelCommitter>(HullLogCtx, HullDbCommitterCtx, RTCtx->LevelIndex,
                ctx.SelfID, std::move(chunksAdded), std::move(deleteChunks), std::move(removedHugeBlobs),
                std::move(allocatedHugeBlobs), wId);
            TActorId committerID = ctx.RegisterWithSameMailbox(committer.release());
            ActiveActors.Insert(committerID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            // drop prev slice, some snapshot can still have a pointer to it
            prevSlice.Drop();
        }

        void LogRemovedHugeBlobs(const TActorContext &ctx, const TDiskPartVec &vec, bool level) const {
            for (const auto &x : vec) {
                LOG_DEBUG(ctx, NKikimrServices::BS_HULLHUGE,
                          VDISKP(HullDs->HullCtx->VCtx, "%s: LogRemovedHugeBlobs: one slot: addr# %s level# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                x.ToString().data(), (level ? "true" : "false")));
            }
        }

        void Handle(typename THullChange::TPtr &ev, const TActorContext &ctx, ui64 wId = 0) {
            if (!wId) {
                ActiveActors.Erase(ev->Sender);
            }
            THullChange *msg = ev->Get();

            if (!msg->FreedHugeBlobs.Empty() && !wId && !msg->Aborted) {
                const ui64 cookie = NextPreCompactCookie++;
                LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, HullDs->HullCtx->VCtx->VDiskLogPrefix
                    << "requesting PreCompact for THullChange");
                ctx.Send(HullLogCtx->HugeKeeperId, new TEvHugePreCompact, 0, cookie);
                PreCompactCallbacks.emplace(cookie, [this, ev](ui64 wId, const TActorContext& ctx) mutable {
                    Y_VERIFY_S(wId, HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, HullDs->HullCtx->VCtx->VDiskLogPrefix
                        << "got PreCompactResult for THullChange, wId# " << wId);
                    Handle(ev, ctx, wId);
                });
                return;
            }

            // NOTE: when we run committer (Fresh or Level) we allocate Lsn and
            //       perform LevelIndex serialization in this handler to _guarantee_ order
            //       of log messages

            // handle commit msg differently
            if (msg->FreshSegment) {
                TStringStream dbg;
                dbg << "{commiter# fresh"
                    << " firstLsn# "<< msg->FreshSegment->GetFirstLsn()
                    << " lastLsn# " << msg->FreshSegment->GetLastLsn()
                    << "}";

                // update compacted lsn
                const ui64 lastLsnFromFresh = msg->FreshSegment->GetLastLsn();
                if (lastLsnFromFresh > 0)
                    RTCtx->LevelIndex->UpdateCompactedLsn(lastLsnFromFresh);
                // check huge blobs
                if (Config->CheckHugeBlobs) {
                    TDiskPartVec checkVec = THullOpUtil::FindRemovedHugeBlobsAfterFreshCompaction(
                        HullDs->HullCtx->VCtx->VDiskLogPrefix, ctx, msg->FreshSegment, msg->SegVec);
                    CheckRemovedHugeBlobs(ctx, msg->FreedHugeBlobs, checkVec, false);
                    LogRemovedHugeBlobs(ctx, msg->FreedHugeBlobs, false);
                }
                // remove fresh segment
                RTCtx->LevelIndex->FreshCompactionSstCreated(std::move(msg->FreshSegment));

                // put new sstable into zero level
                if (msg->SegVec.Get()) {
                    for (auto &seg : msg->SegVec->Segments)
                        RTCtx->LevelIndex->InsertSstAtLevel0(seg, HullDs->HullCtx);
                }

                // run fresh committer
                auto committer = std::make_unique<TAsyncFreshCommitter>(HullLogCtx, HullDbCommitterCtx, RTCtx->LevelIndex,
                        ctx.SelfID, std::move(msg->CommitChunks), std::move(msg->ReservedChunks),
                        std::move(msg->FreedHugeBlobs), std::move(msg->AllocatedHugeBlobs), dbg.Str(), wId);
                auto aid = ctx.RegisterWithSameMailbox(committer.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            } else {
                Y_VERIFY_S(RTCtx->LevelIndex->GetCompState() == TLevelIndexBase::StateCompInProgress,
                    HullDs->HullCtx->VCtx->VDiskLogPrefix);

                // assign VolatileOrderId for any new SSTables at level 0 to allow merging them to level 0 below
                if (const auto& cs = CompactionTask->CompactSsts; cs.TargetLevel == 0) {
                    for (auto& seg : msg->SegVec->Segments) {
                        const ui64 prev = std::exchange(seg->VolatileOrderId,
                            ++RTCtx->LevelIndex->CurSlice->Ctx->VolatileOrderId);
                        Y_VERIFY_S(prev == 0, HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    }
                }

                CompactionTask->CompactSsts.CompactionFinished(std::move(msg->SegVec),
                    std::move(msg->FreedHugeBlobs), std::move(msg->AllocatedHugeBlobs), msg->Aborted);

                if (msg->Aborted) { // if the compaction was aborted, ensure there was no index change
                    Y_VERIFY_S(CompactionTask->GetSstsToAdd().Empty(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(CompactionTask->GetSstsToDelete().Empty(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(CompactionTask->GetHugeBlobsToDelete().Empty(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(!msg->CommitChunks, HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    Y_VERIFY_S(!msg->FreshSegment, HullDs->HullCtx->VCtx->VDiskLogPrefix);
                } else {
                    Y_VERIFY_S(!CompactionTask->GetSstsToDelete().Empty(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
                }

                ApplyCompactionResult(ctx, std::move(msg->CommitChunks), std::move(msg->ReservedChunks), wId);
            }

            RTCtx->LevelIndex->UpdateLevelStat(LevelStat);
        }

        THashMap<ui64, std::function<void(ui64, const TActorContext&)>> PreCompactCallbacks;
        ui64 NextPreCompactCookie = 1;

        void Handle(TEvHugePreCompactResult::TPtr ev, const TActorContext& ctx) {
            const auto it = PreCompactCallbacks.find(ev->Cookie);
            Y_VERIFY_S(it != PreCompactCallbacks.end(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
            it->second(ev->Get()->WId, ctx);
            PreCompactCallbacks.erase(it);
        }

        void Handle(typename TFreshAppendixCompactionDone::TPtr& ev, const TActorContext& ctx) {
            auto newJob = ev->Get()->Job.ApplyCompactionResult();
            if (!newJob.Empty()) {
                RunFreshAppendixCompaction<TKey, TMemRec>(ctx, HullDs->HullCtx->VCtx, ctx.SelfID, std::move(newJob));
            }
        }

        void Handle(TEvAddBulkSst::TPtr& ev, const TActorContext& ctx) {
            TEvAddBulkSst *msg = ev->Get();
            const auto oneAddition = msg->Essence.EnsureOnlyOneSst<TKey, TMemRec>();

            // move level-0 SSTable segment into uncommitted set and spawn committer actor
            Y_VERIFY_S(oneAddition.Sst->IsLoaded(), HullDs->HullCtx->VCtx->VDiskLogPrefix);
            RTCtx->LevelIndex->UncommittedReplSegments.push_back(oneAddition.Sst);

            auto actor = std::make_unique<TAsyncReplSstCommitter>(HullLogCtx, HullDbCommitterCtx, RTCtx->LevelIndex,
                    ctx.SelfID, std::move(msg->ChunksToCommit), std::move(msg->ReservedChunks),
                    oneAddition.Sst, oneAddition.RecsNum, msg->NotifyId);
            auto aid = ctx.RegisterWithSameMailbox(actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(THullCommitFinished::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            switch (ev->Get()->Type) {
                case THullCommitFinished::CommitLevel:
                    Y_VERIFY_DEBUG_S(RTCtx->LevelIndex->GetCompState() == TLevelIndexBase::StateWaitCommit,
                        HullDs->HullCtx->VCtx->VDiskLogPrefix);
                    RTCtx->LevelIndex->SetCompState(TLevelIndexBase::StateNoComp);
                    RTCtx->LevelIndex->PrevEntryPointLsn = ui64(-1);
                    FullCompactionState.Compacted(ctx, CompactionTask->FullCompactionInfo);
                    CompactionTask->Clear();
                    ScheduleCompaction(ctx);
                    break;
                case THullCommitFinished::CommitFresh:
                    ProcessFreshOnlyCompactQ(ctx);
                    ScheduleCompaction(ctx, FullCompactionState.Enabled());
                    break;
                case THullCommitFinished::CommitAdvanceLsn:
                    AdvanceCommitInProgress = false;
                    break;
                case THullCommitFinished::CommitReplSst:
                    break;
                default:
                    Y_ABORT("Unexpected case");
            }

            // notify skeleton about finished compaction
            ctx.Send(RTCtx->SkeletonId, new TEvCompactionFinished());

            // notify HullLogCutterNotifier
            ctx.Send(RTCtx->GetLogNotifierActorId(), new TEvents::TEvCompleted());
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            const ui64 freeUpToLsn = ev->Get()->FreeUpToLsn;
            RTCtx->SetFreeUpToLsn(freeUpToLsn);
            // we check if we need to start fresh compaction, FreeUpToLsn influence our decision
            const bool freshCompStarted = ScheduleCompaction(ctx, false);
            // just for valid info output to the log
            bool moveEntryPointStarted = false;
            if (!freshCompStarted && !AdvanceCommitInProgress) {
                // move entry point if required
                const ui64 entryPoint = Min(RTCtx->LevelIndex->CurEntryPointLsn, RTCtx->LevelIndex->PrevEntryPointLsn);
                if (entryPoint == ui64(-1) || freeUpToLsn > entryPoint) {
                    TStringStream dbg;
                    dbg << "{commiter# advance"
                        << " entryPoint# "<< entryPoint
                        << " freeUpToLsn# " << freeUpToLsn
                        << "}";
                    auto aid = ctx.RegisterWithSameMailbox(new TAsyncAdvanceLsnCommitter(HullLogCtx, HullDbCommitterCtx,
                        RTCtx->LevelIndex, ctx.SelfID, dbg.Str()));
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                    AdvanceCommitInProgress = true;
                    moveEntryPointStarted = true;
                }
            }

            // if we don't start compaction we notify HullLogCutterNotifier; we need it at least for
            // process startup to initialize LogCutter;
            // anyway we don't get NPDisk::TEvCutLog too often, once per log chunk written
            bool justNotifyLogCutter = false;
            if (!freshCompStarted && !AdvanceCommitInProgress && !moveEntryPointStarted) {
                // notify HullLogCutterNotifier
                justNotifyLogCutter = true;
                ctx.Send(RTCtx->GetLogNotifierActorId(), new TEvents::TEvCompleted());
            }

            LOG_DEBUG(ctx, NKikimrServices::BS_LOGCUTTER,
                VDISKP(HullDs->HullCtx->VCtx, "TLevelIndexActor::Handle(NPDisk::TEvCutLog): freshCompStarted# %d"
                    " moveEntryPointStarted# %d justNotifyLogCutter# %d freeUpToLsn# %" PRIu64
                    " CurEntryPointLsn# %" PRIu64 " PrevEntryPointLsn# %" PRIu64,
                    int(freshCompStarted), int(moveEntryPointStarted), int(justNotifyLogCutter),
                    freeUpToLsn, RTCtx->LevelIndex->CurEntryPointLsn, RTCtx->LevelIndex->PrevEntryPointLsn));
        }

        std::deque<std::pair<ui64, TEvHullCompact::TPtr>> FreshOnlyCompactQ;
        ui64 ForceFreshCompactLsn = 0; // latest fresh-only compaction LSN

        void Handle(TEvHullCompact::TPtr &ev, const TActorContext &ctx) {
            const ui64 confirmedLsn = RTCtx->LsnMngr->GetConfirmedLsnForHull();
            auto *msg = ev->Get();
            STLOG(PRI_INFO, BS_HULLCOMP, VDHC01, VDISKP(HullDs->HullCtx->VCtx, "TEvHullCompact"),
                (ConfirmedLsn, confirmedLsn), (Msg, *msg),
                (CompState, TLevelIndexBase::LevelCompStateToStr(RTCtx->LevelIndex->GetCompState())));
            Y_VERIFY_S(TKeyToEHullDbType<TKey>() == msg->Type, HullDs->HullCtx->VCtx->VDiskLogPrefix);

            Y_VERIFY_S(ForceFreshCompactLsn <= confirmedLsn, HullDs->HullCtx->VCtx->VDiskLogPrefix);
            ForceFreshCompactLsn = confirmedLsn;

            switch (msg->Mode) {
                using E = decltype(msg->Mode);

                case E::FULL:
                    FullCompactionState.FullCompactionTask(confirmedLsn, ctx.Now(), msg->Type, msg->RequestId, ev->Sender,
                        std::move(msg->TablesToCompact));
                    ScheduleCompaction(ctx);
                    break;

                case E::FRESH_ONLY:
                    FreshOnlyCompactQ.emplace_back(confirmedLsn, ev);
                    ProcessFreshOnlyCompactQ(ctx);
                    ScheduleCompaction(ctx, false);
                    break;
            }
        }

        void ProcessFreshOnlyCompactQ(const TActorContext& ctx) {
            for (; !FreshOnlyCompactQ.empty(); FreshOnlyCompactQ.pop_front()) {
                if (auto& [lsn, ev] = FreshOnlyCompactQ.front(); RTCtx->LevelIndex->IsWrittenToSstBeforeLsn(lsn)) {
                    ctx.Send(ev->Sender, new TEvHullCompactResult(ev->Get()->Type, ev->Get()->RequestId), 0, ev->Cookie);
                } else {
                    break;
                }
            }
        }

        void HandlePoison(const TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        void HandlePermitGarbageCollection(const TActorContext& /*ctx*/) {
            AllowGarbageCollection = true;
        }

        void Handle(TEvMinHugeBlobSizeUpdate::TPtr ev, const TActorContext& /*ctx*/) {
            MinHugeBlobInBytes = ev->Get()->MinHugeBlobInBytes;
        }

        STRICT_STFUNC(StateFunc,
            HFunc(THullCommitFinished, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvHullCompact, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HTemplFunc(THullChange, Handle)
            HTemplFunc(TFreshAppendixCompactionDone, Handle)
            HTemplFunc(TEvAddBulkSst, Handle)
            HTemplFunc(TSelected, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvBlobStorage::EvPermitGarbageCollection, HandlePermitGarbageCollection)
            HFunc(TEvHugePreCompactResult, Handle)
            HFunc(TEvMinHugeBlobSizeUpdate, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_ASYNC_LEVEL_INDEX;
        }

        TLevelIndexActor(
                TIntrusivePtr<TVDiskConfig> config,
                TIntrusivePtr<THullDs> hullDs,
                std::shared_ptr<THullLogCtx> hullLogCtx,
                THugeBlobCtxPtr hugeBlobCtx,
                ui32 minHugeBlobInBytes,
                TActorId loggerId,
                std::shared_ptr<TRunTimeCtx> rtCtx,
                std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep)
            : TActorBootstrapped<TThis>()
            , Config(std::move(config))
            , HullDs(std::move(hullDs))
            , HullLogCtx(std::move(hullLogCtx))
            , RTCtx(std::move(rtCtx))
            , SyncLogFirstLsnToKeep(std::move(syncLogFirstLsnToKeep))
            , Boundaries(new NHullComp::TBoundaries(RTCtx->PDiskCtx->Dsk->ChunkSize,
                                                    HullDs->HullCtx->HullCompLevel0MaxSstsAtOnce,
                                                    HullDs->HullCtx->HullCompSortedPartsNum,
                                                    Config->Level0UseDreg))
            , HullDbCommitterCtx(new THullDbCommitterCtx(RTCtx->PDiskCtx,
                                                    HullDs->HullCtx,
                                                    RTCtx->LsnMngr,
                                                    loggerId,
                                                    HullLogCtx->HugeKeeperId,
                                                    RTCtx->SkeletonId))
            , CompactionTask(new TCompactionTask)
            , ActiveActors(RTCtx->LevelIndex->ActorCtx->ActiveActors)
            , LevelStat(HullDs->HullCtx->VCtx->VDiskCounters)
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , MinHugeBlobInBytes(minHugeBlobInBytes)
        {}
    };

    NActors::IActor* CreateLogoBlobsActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            THugeBlobCtxPtr hugeBlobCtx,
            ui32 minHugeBlobInBytes,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyLogoBlob, TMemRecLogoBlob>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep) {
        return new TLevelIndexActor<TKeyLogoBlob, TMemRecLogoBlob>(config, hullDs, hullLogCtx, std::move(hugeBlobCtx),
            minHugeBlobInBytes, loggerId, rtCtx,syncLogFirstLsnToKeep);
    }

    NActors::IActor* CreateBlocksActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyBlock, TMemRecBlock>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep) {
        return new TLevelIndexActor<TKeyBlock, TMemRecBlock>(config, hullDs, hullLogCtx, nullptr, 0, loggerId, rtCtx,
            syncLogFirstLsnToKeep);
    }

    NActors::IActor* CreateBarriersActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyBarrier, TMemRecBarrier>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep) {
        return new TLevelIndexActor<TKeyBarrier, TMemRecBarrier>(config, hullDs, hullLogCtx, nullptr, 0, loggerId, rtCtx,
            syncLogFirstLsnToKeep);
    }
}
