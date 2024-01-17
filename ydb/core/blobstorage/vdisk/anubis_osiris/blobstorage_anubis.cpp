#include "blobstorage_anubis.h"
#include "blobstorage_anubis_algo.h"
#include "blobstorage_anubis_osiris.h"
#include "blobstorage_anubisrunner.h"
#include "blobstorage_anubisfinder.h"
#include "blobstorage_anubisproxy.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisQuantumActor
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisQuantumActor : public TActorBootstrapped<TAnubisQuantumActor> {

        // TResult -- actor's result (status)
        struct TResult {
            bool Finished = false;
            TLogoBlobID Pos;
            TAnubisIssues Issues;

            TResult() = default;
            TResult(bool finished, const TLogoBlobID &pos)
                : Finished(finished)
                , Pos(pos)
            {}
        };

        TIntrusivePtr<THullCtx> HullCtx;
        TQueueActorMapPtr QueueActorMapPtr;
        TActiveActors ActiveActors;
        const TActorId ParentId;
        const TActorId SkeletonId;
        TLogoBlobID Pos; // start position
        TLogoBlobsSnapshot LogoBlobsSnap;
        TBarriersSnapshot BarriersSnap;
        TResult Result;
        ui32 MessagesSentToPeers = 0;

        TBlobsToRemove BlobsToRemove;
        ui64 InFly = 0;
        const ui64 MaxInFly = 0;
        // Manages blobs check status and decides what to delete
        TBlobsStatusMngr BlobsStatusMngr;

        friend class TActorBootstrapped<TAnubisQuantumActor>;

        ////////////////////////////////////////////////////////////////////////
        // BOOTSTRAP
        ////////////////////////////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            // prepare a list of candidates
            std::unique_ptr<IActor> finder(CreateAnubisCandidatesFinder(HullCtx, ctx.SelfID, Pos, std::move(LogoBlobsSnap),
                    std::move(BarriersSnap)));
            TActorId aid = RunInBatchPool(ctx, finder.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            Become(&TThis::WaitForCandidatesStateFunc);
        }

        ////////////////////////////////////////////////////////////////////////
        // WAIT FOR CANDIDATES
        ////////////////////////////////////////////////////////////////////////
        STRICT_STFUNC(WaitForCandidatesStateFunc,
                      HFunc(TEvAnubisCandidates, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvActorDied, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

        void Handle(TEvAnubisCandidates::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            bool finished = ev->Get()->Candidates.Finished();
            Result = TResult(finished, ev->Get()->Candidates.Pos);

            if (finished) {
                Finish(ctx);
            } else {
                // store candidates locally
                BlobsStatusMngr.SetupCandidates(std::move(ev->Get()->Candidates.Candidates));
                CommunicateWithPeers(ctx);
                Become(&TThis::WaitPeersReply);
            }
        }

        void CommunicateWithPeers(const TActorContext &ctx) {
            Y_ABORT_UNLESS(MessagesSentToPeers == 0);
            for (const auto &x : HullCtx->VCtx->Top->GetVDisks()) {
                if (HullCtx->VCtx->ShortSelfVDisk != x.VDiskIdShort) {
                    // not self
                    TVector<TLogoBlobID> checkIds;
                    // find out candidates belongs to this x.VDiskIdShort
                    checkIds = BlobsStatusMngr.GetLogoBlobIdsToCheck(x.VDiskIdShort);

                    if (!checkIds.empty()) {
                        // send a check message to proxy if have something to check
                        auto it = QueueActorMapPtr->find(x.VDiskIdShort);
                        Y_ABORT_UNLESS(it != QueueActorMapPtr->end());
                        ctx.Send(it->second, new TEvAnubisVGet(std::move(checkIds)));
                        ++MessagesSentToPeers;
                    }
                }
            }

            // must send several messages or what we are checking?
            Y_ABORT_UNLESS(MessagesSentToPeers != 0);
        }

        ////////////////////////////////////////////////////////////////////////
        // WAIT FOR PEERS REPLY
        ////////////////////////////////////////////////////////////////////////
        STRICT_STFUNC(WaitPeersReply,
                      HFunc(TEvAnubisVGetResult, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvActorDied, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

        void Handle(TEvAnubisVGetResult::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(MessagesSentToPeers > 0);
            --MessagesSentToPeers;

            // mark ids with NODATA
            const auto *msg = ev->Get();
            const auto &record = msg->Ev->Get()->Record;
            BlobsStatusMngr.UpdateStatusForVDisk(msg->VDiskIdShort, record);
            if (record.GetStatus() != NKikimrProto::OK) {
                // we didn't get reply from vd, use it information for resheduling
                // the next iteration later
                Result.Issues.NotOKAnswersFromPeers = true;
            }

            // check have we finished
            if (MessagesSentToPeers == 0) {
                TVector<TLogoBlobID> forRemoval = BlobsStatusMngr.BlobsToRemove();
                if (forRemoval.empty()) {
                    Finish(ctx);
                } else {
                    RemoveBlobs(ctx, std::move(forRemoval));
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // REMOVE BLOBS AND WAIT FOR CONFIRMATION
        ////////////////////////////////////////////////////////////////////////
        STRICT_STFUNC(WaitWriteCompletion,
                      HFunc(TEvAnubisOsirisPutResult, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvActorDied, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

        void RemoveBlobs(const TActorContext &ctx, TVector<TLogoBlobID> &&forRemoval) {
            Y_ABORT_UNLESS(!forRemoval.empty());

            Become(&TThis::WaitWriteCompletion);
            BlobsToRemove = TBlobsToRemove(std::move(forRemoval));
            WriteDeleteCommands(ctx);
        }

        void WriteDeleteCommands(const TActorContext& ctx) {
            // send up to MaxInFly
            while (BlobsToRemove.Valid() && InFly < MaxInFly) {
                const TLogoBlobID &id = BlobsToRemove.Get();
                Y_ABORT_UNLESS(id.PartId() == 0);
                LOG_ERROR(ctx, BS_SYNCER,
                          VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "TAnubisQuantumActor: DELETE: id# %s", id.ToString().data()));
                ctx.Send(SkeletonId, new TEvAnubisOsirisPut(id));
                ++InFly;

                BlobsToRemove.Next();
            }
        }

        void Handle(TEvAnubisOsirisPutResult::TPtr& ev, const TActorContext& ctx) {
            // check reply and notify about error
            if (ev->Get()->Status != NKikimrProto::OK) {
                Result.Issues.LocalWriteErrors = true;
                LOG_ERROR(ctx, BS_SYNCER,
                          VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "TAnubisQuantumActor: local write failed: Status# %s",
                                NKikimrProto::EReplyStatus_Name(ev->Get()->Status).data()));
            }

            --InFly;
            // scan and send messages up to MaxInFly
            WriteDeleteCommands(ctx);
            // check if we need to finish
            if (InFly == 0) {
                Finish(ctx);
            }
        }


        ////////////////////////////////////////////////////////////////////////
        // FINISH
        ////////////////////////////////////////////////////////////////////////
        void Finish(const TActorContext &ctx) {
            ctx.Send(ParentId, new TEvAnubisQuantumDone(Result.Finished,
                                                        Result.Pos,
                                                        Result.Issues));
            Die(ctx);
        }


        ////////////////////////////////////////////////////////////////////////
        // COMMON
        ////////////////////////////////////////////////////////////////////////
        // This handler is called when TAnubisHttpInfoActor is finished
        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // Handle TEvHttpInfo
        ////////////////////////////////////////////////////////////////////////
        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
            TStringStream str;
            HTML(str) {
                str << "Pos: " << Pos << "<br>";
            }

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncerInfoId));
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubisQuantumActor(const TIntrusivePtr<THullCtx> &hullCtx,
                            const TQueueActorMapPtr &queueActorMapPtr,
                            const TActorId &parentId,
                            const TActorId &skeletonId,
                            const TLogoBlobID &pos,
                            TLogoBlobsSnapshot &&logoBlobsSnap,
                            TBarriersSnapshot &&barriersSnap,
                            ui64 anubisOsirisMaxInFly)
            : TActorBootstrapped<TAnubisQuantumActor>()
            , HullCtx(hullCtx)
            , QueueActorMapPtr(queueActorMapPtr)
            , ParentId(parentId)
            , SkeletonId(skeletonId)
            , Pos(pos)
            , LogoBlobsSnap(std::move(logoBlobsSnap))
            , BarriersSnap(std::move(barriersSnap))
            , MaxInFly(anubisOsirisMaxInFly)
            , BlobsStatusMngr(HullCtx->VCtx->Top.get())
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TAnubis
    ////////////////////////////////////////////////////////////////////////////
    class TAnubis : public TActorBootstrapped<TAnubis> {
        TIntrusivePtr<THullCtx> HullCtx;
        const TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        // mapping from TVDiskIdShort to proxy
        TQueueActorMapPtr QueueActorMapPtr;
        TActiveActors ActiveActors;
        const TActorId ParentId;
        const TActorId SkeletonId;
        const ui32 ReplInterconnectChannel;
        const ui64 AnubisOsirisMaxInFly;
        TLogoBlobID Pos;
        ui32 Quantum = 0;
        TInstant StartTime;
        TAnubisIssues Issues;

        friend class TActorBootstrapped<TAnubis>;

        void CreateQueueActorMap(const TActorContext &ctx) {
            QueueActorMapPtr = std::make_shared<TQueueActorMap>();
            for (const auto &x : HullCtx->VCtx->Top->GetVDisks()) {
                if (HullCtx->VCtx->ShortSelfVDisk != x.VDiskIdShort) {
                    auto aid = ctx.Register(CreateAnubisProxy(HullCtx->VCtx, GInfo, x.VDiskIdShort, ReplInterconnectChannel));
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                    (*QueueActorMapPtr)[x.VDiskIdShort] = aid;
                }
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            StartTime = TAppData::TimeProvider->Now();
            CreateQueueActorMap(ctx);
            NextRound(ctx);
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr &ev, const TActorContext &ctx) {
            Become(&TThis::QuantumStateFunc);
            auto &snap = ev->Get()->Snap;
            auto a = std::make_unique<TAnubisQuantumActor>(HullCtx, QueueActorMapPtr, ctx.SelfID, SkeletonId, Pos,
                    std::move(snap.LogoBlobsSnap), std::move(snap.BarriersSnap), AnubisOsirisMaxInFly);
            auto aid = ctx.Register(a.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvAnubisQuantumDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);

            // aggregate not OK answers
            Issues.Merge(ev->Get()->Issues);

            if (ev->Get()->Finished) {
                // finish job
                Finish(ctx);
            } else {
                // next round
                Pos = ev->Get()->Pos;
                NextRound(ctx);
            }
        }

        void Finish(const TActorContext &ctx) {
            ctx.Send(ParentId, new TEvAnubisDone(Issues));
            Die(ctx);
        }

        void NextRound(const TActorContext &ctx) {
            // get snapshot from Skeleton
            ++Quantum;
            const bool indexSnap = true;
            ctx.Send(SkeletonId, new TEvTakeHullSnapshot(indexSnap));
            Become(&TThis::WaitForSnapshotStateFunc);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            TStringStream str;
            HTML(str) {
                str << "Pos: " << Pos.ToString() << "<br>";
                str << "Quantum: " << Quantum << "<br>";
                str << "StartTime: " << StartTime << "<br>";
            }

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncerInfoId));
        }


        STRICT_STFUNC(WaitForSnapshotStateFunc,
                      HFunc(TEvTakeHullSnapshotResult, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

        STRICT_STFUNC(QuantumStateFunc,
                      HFunc(TEvAnubisQuantumDone, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubis(const TIntrusivePtr<THullCtx> &hullCtx,
                const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                const TActorId &parentId,
                const TActorId &skeletonId,
                ui32 replInterconnectChannel,
                ui64 anubisOsirisMaxInFly)
            : TActorBootstrapped<TAnubis>()
            , HullCtx(hullCtx)
            , GInfo(ginfo)
            , ParentId(parentId)
            , SkeletonId(skeletonId)
            , ReplInterconnectChannel(replInterconnectChannel)
            , AnubisOsirisMaxInFly(anubisOsirisMaxInFly)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // ANUBIS ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateAnubis(const TIntrusivePtr<THullCtx> &hullCtx,
                         const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                         const TActorId &parentId,
                         const TActorId &skeletonId,
                         ui32 replInterconnectChannel,
                         ui64 anubisOsirisMaxInFly) {
        return new TAnubis(hullCtx, ginfo, parentId, skeletonId, replInterconnectChannel, anubisOsirisMaxInFly);
    }


} // NKikimr
