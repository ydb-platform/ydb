#include "blobstorage_synclog.h"
#include "blobstorage_synclogdata.h"
#include "blobstorage_synclogmsgwriter.h"
#include "blobstorage_synclogkeeper.h"
#include "blobstorage_synclogrecovery.h"
#include "blobstorage_syncloghttp.h"
#include "blobstorage_synclogreader.h"
#include "blobstorage_synclog_private_events.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_status.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncLog;

namespace NKikimr {

    namespace NSyncLog {


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogGetLocalStatusActor
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogGetLocalStatusActor
            : public TActorBootstrapped<TSyncLogGetLocalStatusActor>
        {
            TIntrusivePtr<TSyncLogCtx> SlCtx;
            TEvLocalStatus::TPtr Ev;
            const TActorId NotifyId;
            const TActorId KeeperId;

            friend class TActorBootstrapped<TSyncLogGetLocalStatusActor>;

            void Bootstrap(const TActorContext &ctx) {
                Become(&TThis::StateFunc);
                ctx.Send(KeeperId, new TEvSyncLogLocalStatus());
            }

            void Handle(TEvSyncLogLocalStatusResult::TPtr &ev, const TActorContext &ctx) {
                std::unique_ptr<TEvLocalStatusResult> result(new TEvLocalStatusResult());
                NKikimrBlobStorage::TSyncLogStatus *rec = result->Record.MutableSyncLogStatus();
                const TLogEssence &e = ev->Get()->Essence;
                rec->SetLogStartLsn(e.LogStartLsn);
                rec->SetMemLogEmpty(e.MemLogEmpty);
                rec->SetDiskLogEmpty(e.DiskLogEmpty);
                rec->SetFirstMemLsn(e.FirstMemLsn);
                rec->SetLastMemLsn(e.LastMemLsn);
                rec->SetFirstDiskLsn(e.FirstDiskLsn);
                rec->SetLastDiskLsn(e.LastDiskLsn);

                ctx.Send(Ev->Sender, result.release());
                ctx.Send(NotifyId, new TEvents::TEvCompleted());
                Die(ctx);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvSyncLogLocalStatusResult, Handle)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_LOCAL_STATUS;
            }

            TSyncLogGetLocalStatusActor(TIntrusivePtr<TSyncLogCtx> &slCtx,
                                        TEvLocalStatus::TPtr &ev,
                                        const TActorId &notifyId,
                                        const TActorId &keeperId)
                : TActorBootstrapped<TSyncLogGetLocalStatusActor>()
                , SlCtx(slCtx)
                , Ev(ev)
                , NotifyId(notifyId)
                , KeeperId(keeperId)
            {}
        };


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogActor
        ////////////////////////////////////////////////////////////////////////////
        // Recovery Notes:
        // 1. We have some data on disk, we have index ref in log, read it
        // 2. Apply records from recovery log
        // 3. There is no need to recover sync position (we don't store other VDisks
        //    position, its their responsibility)
        class TSyncLogActor : public TActorBootstrapped<TSyncLogActor> {
        private:
            TIntrusivePtr<TSyncLogCtx> SlCtx;
            TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
            // actual self VDiskID, it is changed during BlobStorage Group reconfiguration
            TVDiskID SelfVDiskId;
            std::unique_ptr<TSyncLogRepaired> Repaired;
            TSyncLogNeighborsPtr NeighborsPtr;
            const TVDiskIncarnationGuid VDiskIncarnationGuid;
            TActorId KeeperId;
            TMaybe<ui64> DbBirthLsn;
            TActiveActors ActiveActors;

            friend class TActorBootstrapped<TSyncLogActor>;

            ui64 GetDbBirthLsn() {
                Y_ABORT_UNLESS(DbBirthLsn.Defined());
                return *DbBirthLsn;
            }

            void Bootstrap(const TActorContext &ctx) {
                NeighborsPtr = MakeIntrusive<TSyncLogNeighbors>(SlCtx->VCtx->ShortSelfVDisk,
                                                                SlCtx->VCtx->Top,
                                                                SlCtx->VCtx->VDiskLogPrefix,
                                                                ctx.ExecutorThread.ActorSystem);
                KeeperId = ctx.Register(CreateSyncLogKeeperActor(SlCtx, std::move(Repaired)));
                ActiveActors.Insert(KeeperId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                TThis::Become(&TThis::StateFunc);
            }

            void Handle(TEvBlobStorage::TEvVSync::TPtr &ev, const TActorContext &ctx) {
                ++SlCtx->IFaceMonGroup.SyncReadMsgs();

                TInstant now = TAppData::TimeProvider->Now();
                const NKikimrBlobStorage::TEvVSync &record = ev->Get()->Record;
                TSyncState oldSyncState = SyncStateFromSyncState(record.GetSyncState());
                const TVDiskID sourceVDisk = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                const TVDiskID targetVDisk = VDiskIDFromVDiskID(record.GetTargetVDiskID());

                // check that the disk is from this group
                if (!SelfVDiskId.SameGroupAndGeneration(sourceVDisk) ||
                    !SelfVDiskId.SameDisk(targetVDisk)) {
                    LOG_WARN(ctx, BS_SYNCLOG,
                              VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "Handle(TEvSyncLogRead): check vdisk id failed; "
                                    "SelfVDiskId# %s sourceVDisk# %s targetVDisk# %s",
                                    SelfVDiskId.ToString().data(), sourceVDisk.ToString().data(),
                                    targetVDisk.ToString().data()));

                    auto result = std::make_unique<TEvBlobStorage::TEvVSyncResult>(NKikimrProto::RACE, SelfVDiskId,
                        TSyncState(), true, SlCtx->VCtx->GetOutOfSpaceState().GetLocalStatusFlags(), now,
                        SlCtx->CountersMonGroup.VDiskCheckFailedPtr(), nullptr, ev->GetChannel());
                    SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, SlCtx->VCtx, {});
                    return;
                }

                Y_DEBUG_ABORT_UNLESS(sourceVDisk != SelfVDiskId);

                // handle locks
                if (NeighborsPtr->IsLocked(sourceVDisk)) {
                    // we already have a lock set by this vdisk, i.e. read request up and running;
                    // reply with error, it's an error from their side
                    LOG_ERROR(ctx, BS_SYNCLOG,
                              VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "Handle(TEvSyncLogRead): locked; "
                                    "sourceVDisk# %s targetVDisk# %s",
                                    sourceVDisk.ToString().data(), targetVDisk.ToString().data()));

                    auto result = std::make_unique<TEvBlobStorage::TEvVSyncResult>(NKikimrProto::BLOCKED, SelfVDiskId,
                        TSyncState(), true, SlCtx->VCtx->GetOutOfSpaceState().GetLocalStatusFlags(), now,
                        SlCtx->CountersMonGroup.DiskLockedPtr(), nullptr, ev->GetChannel());
                    SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, SlCtx->VCtx, {});
                    return;
                }

                // check IncarnationGuid and reply with correct (IncarnationGuid, DbBirthLsn)
                // if it has been changed
                if (VDiskIncarnationGuid != oldSyncState.Guid) {
                    LOG_WARN(ctx, BS_SYNCLOG,
                             VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                    "Handle(TEvSyncLogRead): FULL_RECOVER(unequal guid); "
                                   "sourceVDisk# %s targetVDisk# %s oldSyncState# %s"
                                   " DbBirthLsn# %" PRIu64,
                                   sourceVDisk.ToString().data(), targetVDisk.ToString().data(),
                                   oldSyncState.ToString().data(), GetDbBirthLsn()));

                    auto status = NKikimrProto::RESTART;
                    TSyncState syncState(VDiskIncarnationGuid, GetDbBirthLsn());
                    auto result = std::make_unique<TEvBlobStorage::TEvVSyncResult>(status, SelfVDiskId, syncState,
                        true, SlCtx->VCtx->GetOutOfSpaceState().GetLocalStatusFlags(), now,
                        SlCtx->CountersMonGroup.UnequalGuidPtr(), nullptr, ev->GetChannel());
                    SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, SlCtx->VCtx, {});
                    return;
                }

                if (!SlCtx->IsReadOnlyVDisk) {
                    // cut the log (according to confirmed old synced state)
                    CutLog(ctx, sourceVDisk, oldSyncState.SyncedLsn);
                }
                // process the request further asyncronously
                NeighborsPtr->Lock(sourceVDisk, oldSyncState.SyncedLsn);
                auto aid = ctx.Register(CreateSyncLogReaderActor(SlCtx, VDiskIncarnationGuid, ev, ctx.SelfID, KeeperId,
                    SelfVDiskId, sourceVDisk, GetDbBirthLsn(), now));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }

            void Handle(TEvSyncLogPut::TPtr &ev, const TActorContext &ctx) {
                ++SlCtx->IFaceMonGroup.SyncPutMsgs();
                LOG_DEBUG(ctx, BS_SYNCLOG,
                          VDISKP(SlCtx->VCtx->VDiskLogPrefix,
                                "Handle(TEvSyncLogPut): recs# %s",
                                ev->Get()->GetRecs().ToString().data()));
                ctx.Send(ev->Forward(KeeperId));
            }

            void Handle(TEvSyncLogPutSst::TPtr& ev, const TActorContext& ctx) {
                ++SlCtx->IFaceMonGroup.SyncPutSstMsgs();
                ctx.Send(ev->Forward(KeeperId));
            }

            void Handle(TEvSyncLogReadFinished::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ctx);
                NeighborsPtr->Unlock(ev->Get()->VDiskID);
                ActiveActors.Erase(ev->Sender);
            }

            void Handle(TEvSyncLogDbBirthLsn::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ctx);
                DbBirthLsn = ev->Get()->DbBirthLsn;
            }

            void CutLog(const TActorContext &ctx, const TVDiskID &vdisk, ui64 syncedLsn) {
                ui64 currentSyncedLsn = NeighborsPtr->GetSyncedLsn(vdisk);
                if (currentSyncedLsn < syncedLsn) {
                    // store prev min value
                    ui64 prevMinLsn = NeighborsPtr->GlobalSyncedLsn();
                    // update synced lsn and reorder queue
                    NeighborsPtr->UpdateSyncedLsn(vdisk, syncedLsn);
                    // get current min value
                    ui64 curMinLsn = NeighborsPtr->GlobalSyncedLsn();
                    Y_ABORT_UNLESS(prevMinLsn <= curMinLsn,
                             "TSyncLogActor::CutLog: currentSyncedLsn# %" PRIu64
                             " syncedLsn# %" PRIu64 " vdisk# %s prevMinLsn# %" PRIu64
                             " curMinLsn# %" PRIu64,
                             currentSyncedLsn, syncedLsn, vdisk.ToString().data(),
                             prevMinLsn, curMinLsn);
                    if (prevMinLsn < curMinLsn) {
                        ctx.Send(KeeperId, new TEvSyncLogTrim(curMinLsn));
                    }
                }
            }

            void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
                ctx.Send(ev->Forward(KeeperId));
            }

            void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
                Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::SyncLogId);
                auto aid = ctx.RegisterWithSameMailbox(CreateGetHttpInfoActor(SlCtx->VCtx, GInfo, ev, SelfId(), KeeperId,
                    NeighborsPtr));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }

            void Handle(TEvBlobStorage::TEvVBaldSyncLog::TPtr& ev, const TActorContext& ctx) {
                ctx.Send(ev->Forward(KeeperId));
            }

            void Handle(TEvLocalStatus::TPtr &ev, const TActorContext &ctx) {
                auto selfId = ctx.SelfID;
                auto actor = std::make_unique<TSyncLogGetLocalStatusActor>(SlCtx, ev, selfId, KeeperId);
                auto aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }

            // reconfigure BlobStorage Group
            void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ctx);
                auto *msg = ev->Get();
                GInfo = msg->NewInfo;
                Y_ABORT_UNLESS(msg->NewVDiskId == msg->NewInfo->GetVDiskId(SlCtx->VCtx->ShortSelfVDisk));
                SelfVDiskId = msg->NewVDiskId;
            }

            // This handler is called when TSyncLogGetHttpInfoActor or
            // TSyncLogGetLocalStatusActor is finished
            void HandleActorCompletion(TEvents::TEvCompleted::TPtr &ev,
                                       const TActorContext &ctx) {
                Y_UNUSED(ctx);
                ActiveActors.Erase(ev->Sender);
            }

            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                ActiveActors.KillAndClear(ctx);
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvSyncLogPut, Handle)
                HFunc(TEvSyncLogPutSst, Handle)
                HFunc(TEvBlobStorage::TEvVSync, Handle)
                HFunc(TEvSyncLogReadFinished, Handle)
                HFunc(TEvSyncLogDbBirthLsn, Handle)
                HFunc(NPDisk::TEvCutLog, Handle)
                HFunc(NMon::TEvHttpInfo, Handle)
                HFunc(TEvBlobStorage::TEvVBaldSyncLog, Handle)
                HFunc(TEvLocalStatus, Handle)
                HFunc(TEvVGenerationChange, Handle)
                HFunc(TEvents::TEvCompleted, HandleActorCompletion)
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
            )

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNCLOG_ACTOR;
            }

            TSyncLogActor(
                    const TIntrusivePtr<TSyncLogCtx> &slCtx,
                    const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                    const TVDiskID &selfVDiskId,
                    std::unique_ptr<TSyncLogRepaired> repaired)
                : TActorBootstrapped<TSyncLogActor>()
                , SlCtx(slCtx)
                , GInfo(ginfo)
                , SelfVDiskId(selfVDiskId)
                , Repaired(std::move(repaired))
                , NeighborsPtr()
                , VDiskIncarnationGuid(Repaired->SyncLogPtr->Header.VDiskIncarnationGuid)
                , KeeperId()
            {}
        };

    } // NSyncLog

    IActor* CreateSyncLogActor(
            const TIntrusivePtr<TSyncLogCtx> &slCtx,
            const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
            const TVDiskID &selfVDiskId,
            std::unique_ptr<NSyncLog::TSyncLogRepaired> repaired) {
        return new TSyncLogActor(slCtx, ginfo, selfVDiskId, std::move(repaired));
    }

} // NKikimr

