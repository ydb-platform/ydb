#include "blobstorage_synclogkeeper.h"
#include "blobstorage_synclogkeeper_state.h"
#include "blobstorage_synclog.h"
#include "blobstorage_synclog_private_events.h"

#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/sublog.h>
#include <ydb/core/blobstorage/vdisk/common/circlebufstream.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

using namespace NKikimrServices;

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogKeeperActor
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogKeeperActor : public TActorBootstrapped<TSyncLogKeeperActor>
        {
            friend class TActorBootstrapped<TSyncLogKeeperActor>;

            TIntrusivePtr<TSyncLogCtx> SlCtx;
            TSyncLogKeeperState KeepState;
            // Sublog with circle buffer
            TSublog<TCircleBufStringStream<4096>> Sublog;
            // committer actor id, or empty if we don't have commit activity at this moment
            TActorId CommitterId = {};
            // FirstLsnToKeep reported to LogCutter last time
            TMaybe<ui64> LastReportedFirstLsnToKeep;

            void Bootstrap(const TActorContext &ctx) {
                KeepState.Init(
                    std::make_shared<TActorNotify>(ctx.ExecutorThread.ActorSystem, ctx.SelfID),
                    std::make_shared<TActorSystemLoggerCtx>(ctx.ExecutorThread.ActorSystem));
                PerformActions(ctx);
                Become(&TThis::StateFunc);
            }

            ////////////////////////////////////////////////////////////////////////
            // ACTIONS
            ////////////////////////////////////////////////////////////////////////

            // reaction on CutLog event from PDisk, mainly perform commit
            bool PerformCutLogAction(const TActorContext &ctx) {
                auto notCommitHandler = [&ctx, this] (ui64 firstLsnToKeep) {
                    FixateFirstLsnToKeep(ctx, firstLsnToKeep);
                };
                return KeepState.PerformCutLogAction(std::move(notCommitHandler));
            }

            // just trim log based by TrimTailLsn (which is confirmed lsn from peers)
            bool PerformTrimTailAction() {
                const bool hasToCommit = KeepState.PerformTrimTailAction();

                // we don't need to commit because we either remove mem pages or
                // schedule to remove some chunks (but they may be used by snapshots,
                // so wait until TEvSyncLogFreeChunk message)
                Y_ABORT_UNLESS(!hasToCommit);
                return false;
            }

            bool PerformMemOverflowAction() {
                return KeepState.PerformMemOverflowAction();
            }

            bool PerformDeleteChunkAction() {
                return KeepState.PerformDeleteChunkAction();
            }

            bool PerformInitialCommit() {
                return KeepState.PerformInitialCommit();
            }


            ////////////////////////////////////////////////////////////////////////
            // PERFORM ACTIONS
            ////////////////////////////////////////////////////////////////////////
            void PerformActions(const TActorContext &ctx) {
                if (CommitterId || !KeepState.HasDelayedActions()) {
                    // be fast: already committing or has no actions? Return.
                    return;
                }

                bool generateCommit = false;
                generateCommit |= PerformTrimTailAction();
                generateCommit |= PerformCutLogAction(ctx);
                generateCommit |= PerformMemOverflowAction();
                generateCommit |= PerformDeleteChunkAction();
                generateCommit |= PerformInitialCommit();

                if (generateCommit) {
                    Y_ABORT_UNLESS(!CommitterId);
                    // we must save recovery log records after
                    const ui64 recoveryLogConfirmedLsn = SlCtx->LsnMngr->GetConfirmedLsnForSyncLog();
                    // create and run committer
                    TSyncLogKeeperCommitData commitData = KeepState.PrepareCommitData(recoveryLogConfirmedLsn);

                    LOG_DEBUG(ctx, BS_SYNCLOG,
                            VDISKP(SlCtx->VCtx->VDiskLogPrefix, "KEEPER: start committer; commitData# %s",
                                commitData.ToString().data()));

                    CommitterId = ctx.Register(CreateSyncLogCommitter(SlCtx, ctx.SelfID, std::move(commitData)));
                }
            }

            /*

                                                               we are here
                                                                   \/
            ----------------------------------------------------------------------------------> lsns (time)
                                 ^                  ^                             ^
                                 |                  |                             |
                           ConfirmedLsns       Some records infligth          Our entry point lsn (future)

            */

            ////////////////////////////////////////////////////////////////////////
            // HELPERS
            ////////////////////////////////////////////////////////////////////////
            void FixateFirstLsnToKeep(const TActorContext &ctx, ui64 firstLsnToKeep) {
                LOG_DEBUG(ctx, BS_SYNCLOG,
                        VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                            "KEEPER: FixateFirstLsnToKeep: firstLsnToKeep# %" PRIu64 " decomposed# %s",
                            firstLsnToKeep, KeepState.CalculateFirstLsnToKeepDecomposed().data()));

                if (LastReportedFirstLsnToKeep.Empty() || firstLsnToKeep > *LastReportedFirstLsnToKeep) {
                    SlCtx->SyncLogFirstLsnToKeep->Set(firstLsnToKeep);
                    ctx.Send(SlCtx->LogCutterID, new TEvVDiskCutLog(TEvVDiskCutLog::SyncLog, firstLsnToKeep));
                    LastReportedFirstLsnToKeep = firstLsnToKeep;

                    LOG_DEBUG(ctx, BS_SYNCLOG,
                            VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "KEEPER: FixateFirstLsnToKeep: CutLog: firstLsnToKeep# %" PRIu64 " decomposed# %s",
                                firstLsnToKeep, KeepState.CalculateFirstLsnToKeepDecomposed().data()));
                }
            }


            ////////////////////////////////////////////////////////////////////////
            // HANDLERS
            ////////////////////////////////////////////////////////////////////////
            void Handle(TEvSyncLogPut::TPtr &ev, const TActorContext &ctx) {
                KeepState.PutMany(ev->Get()->GetRecs().GetData(), ev->Get()->GetRecs().GetSize());
                PerformActions(ctx);
            }

            void Handle(TEvSyncLogPutSst::TPtr& ev, const TActorContext& ctx) {
                KeepState.PutLevelSegment(ev->Get()->LevelSegment.Get());
                PerformActions(ctx);
            }

            void Handle(TEvSyncLogTrim::TPtr &ev, const TActorContext &ctx) {
                const ui64 trimTailLsn = ev->Get()->Lsn;
                KeepState.TrimTailEvent(trimTailLsn);
                PerformActions(ctx);
            }

            void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
                const ui64 freeUpToLsn = ev->Get()->FreeUpToLsn;
                KeepState.CutLogEvent(freeUpToLsn);
                PerformActions(ctx);
            }

            void Handle(TEvSyncLogFreeChunk::TPtr &ev, const TActorContext &ctx) {
                const ui32 chunkIdx = ev->Get()->ChunkIdx;
                KeepState.FreeChunkEvent(chunkIdx);
                PerformActions(ctx);
            }

            void Handle(TEvSyncLogCommitDone::TPtr &ev, const TActorContext &ctx) {
                auto *msg = ev->Get();
                LOG_DEBUG(ctx, BS_SYNCLOG,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "KEEPER: TEvSyncLogCommitDone: ev# %s",
                                msg->ToString().data()));

                LOG_DEBUG(ctx, BS_LOGCUTTER,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "KEEPER: TEvSyncLogCommitDone: ev# %s",
                                msg->ToString().data()));

                // log commit to Sublog
                Sublog.Log() << ToStringLocalTimeUpToSeconds(ctx.Now())
                    << " Commit done: message# " << msg->ToString().data() << "\n";

                // commit finished
                CommitterId = TActorId();
                const ui64 firstLsnToKeep = KeepState.ApplyCommitResult(msg);

                // If we have done some work to free up recovery log, the layout
                // of SyncLog has been changed. Actually we have written some
                // mem pages to disk, have written new entry point, so
                // firt keep lsn have moved forward even without any actions
                // towards explicit log trimming. It means we don't need to
                // call some 'trim log' function on SyncLog, we can just recalculate
                // firstKeepLsn

                // we notify log cutter about our first lsn to keep in recovery log
                FixateFirstLsnToKeep(ctx, firstLsnToKeep);

                // NOTE: sometimes one commit is not enough to cut the log according FreeUpToLsn
                // several commits are required
                if (!KeepState.FreeUpToLsnSatisfied()) {
                    KeepState.RetryCutLogEvent();
                }

                // perform delayed actions
                PerformActions(ctx);
            }

            void Handle(TEvSyncLogSnapshot::TPtr &ev, const TActorContext &ctx) {
                ++SlCtx->IFaceMonGroup.SyncLogGetSnapshot();
                auto snap = KeepState.GetSyncLogSnapshot();
                TString sublogContent;
                if (ev->Get()->IntrospectionInfo)
                    sublogContent = Sublog.Get();
                ctx.Send(ev->Sender, new TEvSyncLogSnapshotResult(snap, sublogContent));
            }

            void Handle(TEvSyncLogLocalStatus::TPtr &ev, const TActorContext &ctx) {
                ++SlCtx->IFaceMonGroup.SyncLogLocalStatus();
                TLogEssence e;
                KeepState.FillInSyncLogEssence(&e);
                ctx.Send(ev->Sender, new TEvSyncLogLocalStatusResult(e));
            }

            void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                if (CommitterId) {
                    ctx.Send(CommitterId, new TEvents::TEvPoisonPill());
                }
                Die(ctx);
            }

            // for tests/debug purposes only, remove almost all log
            void Handle(TEvBlobStorage::TEvVBaldSyncLog::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                KeepState.BaldLogEvent();
                PerformActions(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvSyncLogPut, Handle)
                HFunc(TEvSyncLogPutSst, Handle)
                HFunc(TEvSyncLogTrim, Handle)
                HFunc(TEvSyncLogFreeChunk, Handle)
                HFunc(TEvSyncLogCommitDone, Handle)
                HFunc(TEvSyncLogSnapshot, Handle)
                HFunc(TEvSyncLogLocalStatus, Handle)
                HFunc(TEvBlobStorage::TEvVBaldSyncLog, Handle)
                HFunc(NPDisk::TEvCutLog, Handle)
                HFunc(TEvents::TEvPoisonPill, Handle)
            )

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_KEEPER;
            }

            TSyncLogKeeperActor(
                    TIntrusivePtr<TSyncLogCtx> slCtx,
                    std::unique_ptr<TSyncLogRepaired> repaired)
                : TActorBootstrapped<TSyncLogKeeperActor>()
                , SlCtx(std::move(slCtx))
                , KeepState(SlCtx->VCtx, std::move(repaired), SlCtx->SyncLogMaxMemAmount, SlCtx->SyncLogMaxDiskAmount,
                    SlCtx->SyncLogMaxEntryPointSize)
            {}
        };

        IActor* CreateSyncLogKeeperActor(
                TIntrusivePtr<TSyncLogCtx> slCtx,
                std::unique_ptr<TSyncLogRepaired> repaired) {
            return new TSyncLogKeeperActor(std::move(slCtx), std::move(repaired));
        }

    } // NSyncLog
} // NKikimr
