#include "blobstorage_syncfullhandler.h"
#include "blobstorage_db.h"
#include "blobstorage_syncfull.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TVSyncFullHandler -- this actor starts on handling TEvVSyncFull requst.
    // It receives last lsn from sync log to make a snapshot of hull database.
    // This actor must be run on the same mailbox as TSkeleton
    ////////////////////////////////////////////////////////////////////////////
    class TVSyncFullHandler : public TActorBootstrapped<TVSyncFullHandler> {
        TIntrusivePtr<TDb> Db;
        TIntrusivePtr<THullCtx> HullCtx;
        const TVDiskID SelfVDiskId;
        const TActorId ParentId;
        std::shared_ptr<THull> Hull;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        TEvBlobStorage::TEvVSyncFull::TPtr Ev;
        const NKikimrBlobStorage::TEvVSyncFull &Record;
        const TVDiskID SourceVDisk;
        const TVDiskID TargetVDisk;
        const TInstant Now;
        const ui64 DbBirthLsn;
        const ui64 ConfirmedLsn;
        TActiveActors ActiveActors;

        // FIXME: rewrite TVSyncFullHandler, use single snapshot for this task,
        //        generate multiple messages per request
        // NOTE:  multiple snapshots can lead to case when we
        //        1. synced blob B with snaphsot Lsn=X
        //        2. synced barriers database with snaphot Lsn=Y, it contained GC command
        //           with Lsn=Z, so that X < Z < Y. GC command also had Keep flag for B
        //        => we deleted B, because it can be collected

        friend class TActorBootstrapped<TVSyncFullHandler>;

        void Bootstrap(const TActorContext &ctx) {
            IFaceMonGroup->SyncFullMsgs()++;

            TActorId recipient = Ev->Sender;
            const ui64 cookie = Ev->Cookie;
            TSyncState clientSyncState(SyncStateFromSyncState(Record.GetSyncState()));

            LOG_DEBUG_S(ctx, BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                    << "TVSyncFullHandler: Bootstrap: fromVDisk# "
                    << VDiskIDFromVDiskID(Record.GetSourceVDiskID())
                    << " fromSyncState# " << clientSyncState.ToString()
                    << " Marker# BSVSFH01");


            // check that the disk is from this group
            if (!SelfVDiskId.SameGroupAndGeneration(SourceVDisk) ||
                !SelfVDiskId.SameDisk(TargetVDisk)) {
                auto result = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(NKikimrProto::ERROR, SelfVDiskId,
                    Record.GetCookie(), Now, IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, Ev->GetChannel());
                SendVDiskResponse(ctx, recipient, result.release(), cookie, HullCtx->VCtx, {});
                Die(ctx);
                return;
            }

            // check disk guid and start from the beginning if it has changed
            if (Db->GetVDiskIncarnationGuid() != clientSyncState.Guid) {
                LOG_DEBUG_S(ctx, BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                        << "TVSyncFullHandler: GUID CHANGED;"
                        << " SourceVDisk# " << SourceVDisk
                        << " DbBirthLsn# " << DbBirthLsn
                        << " Marker# BSVSFH02");
                auto result = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(NKikimrProto::NODATA, SelfVDiskId,
                    TSyncState(Db->GetVDiskIncarnationGuid(), DbBirthLsn), Record.GetCookie(), Now,
                    IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, Ev->GetChannel());
                SendVDiskResponse(ctx, recipient, result.release(), cookie, HullCtx->VCtx, {});
                Die(ctx);
                return;
            }

            Y_DEBUG_ABORT_UNLESS(SourceVDisk != SelfVDiskId);

            Run(ctx, clientSyncState);
        }

        // ask Hull to create a snapshot and to run a job
        void Run(const TActorContext &ctx, const TSyncState &clientSyncState) {
            ui64 syncedLsn = 0;
            if (Ev->Get()->IsInitial()) {
                // this is the first message, so after full sync source node
                // will be synced by lsn obtained from SyncLog
                syncedLsn = Db->LsnMngr->GetConfirmedLsnForSyncLog();
            } else {
                // use old lsn from previous messages
                syncedLsn = clientSyncState.SyncedLsn;
            }

            // parse stage and keys
            TActorId recipient = Ev->Sender;
            const NKikimrBlobStorage::ESyncFullStage stage = Record.GetStage();
            const TLogoBlobID logoBlobFrom = LogoBlobIDFromLogoBlobID(Record.GetLogoBlobFrom());
            const ui64 blockTabletFrom = Record.GetBlockTabletFrom();
            const TKeyBarrier barrierFrom(Record.GetBarrierFrom());

            TSyncState newSyncState(Db->GetVDiskIncarnationGuid(), syncedLsn);
            auto result = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(NKikimrProto::OK, SelfVDiskId,
                newSyncState, Record.GetCookie(), Now, IFaceMonGroup->SyncFullResMsgsPtr(), nullptr,
                Ev->GetChannel());

            // snapshotLsn is _always_ the last confirmed lsn
            THullDsSnap fullSnap = Hull->GetIndexSnapshot();
            LOG_DEBUG_S(ctx, BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                    << "TVSyncFullHandler: ourConfirmedLsn# " << ConfirmedLsn
                    << " syncedLsn# " << syncedLsn
                    << " SourceVDisk# " << SourceVDisk
                    << " Marker# BSVSFH03");

            IActor *actor = CreateHullSyncFullActor(
                Db->Config,
                HullCtx,
                ctx.SelfID,
                SourceVDisk,
                recipient,
                std::move(fullSnap),
                TKeyLogoBlob(logoBlobFrom),
                TKeyBlock(blockTabletFrom),
                barrierFrom,
                stage,
                std::move(result));
            auto aid = ctx.Register(actor);
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            Become(&TThis::StateFunc);
        }

        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvActorDied, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNC_FULL_HANDLER;
        }

        TVSyncFullHandler(const TIntrusivePtr<TDb> &db,
                          const TIntrusivePtr<THullCtx> &hullCtx,
                          const TVDiskID &selfVDiskId,
                          const TActorId &parentId,
                          const std::shared_ptr<THull> &hull,
                          const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                          TEvBlobStorage::TEvVSyncFull::TPtr &ev,
                          const TInstant &now,
                          ui64 dbBirthLsn,
                          ui64 confirmedLsn)
            : TActorBootstrapped<TVSyncFullHandler>()
            , Db(db)
            , HullCtx(hullCtx)
            , SelfVDiskId(selfVDiskId)
            , ParentId(parentId)
            , Hull(hull)
            , IFaceMonGroup(ifaceMonGroup)
            , Ev(ev)
            , Record(Ev->Get()->Record)
            , SourceVDisk(VDiskIDFromVDiskID(Record.GetSourceVDiskID()))
            , TargetVDisk(VDiskIDFromVDiskID(Record.GetTargetVDiskID()))
            , Now(now)
            , DbBirthLsn(dbBirthLsn)
            , ConfirmedLsn(confirmedLsn)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateHullSyncFullHandler
    // VDisk Skeleton Handler for TEvVSyncFull event
    // MUST work on the same mailbox as Skeleton
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateHullSyncFullHandler(const TIntrusivePtr<TDb> &db,
                                      const TIntrusivePtr<THullCtx> &hullCtx,
                                      const TVDiskID &selfVDiskId,
                                      const TActorId &parentId,
                                      const std::shared_ptr<THull> &hull,
                                      const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                                      TEvBlobStorage::TEvVSyncFull::TPtr &ev,
                                      const TInstant &now,
                                      ui64 dbBirthLsn,
                                      ui64 confirmedLsn) {
        return new TVSyncFullHandler(db, hullCtx, selfVDiskId, parentId, hull, ifaceMonGroup, ev, now, dbBirthLsn,
                confirmedLsn);
    }



} // NKikimr
