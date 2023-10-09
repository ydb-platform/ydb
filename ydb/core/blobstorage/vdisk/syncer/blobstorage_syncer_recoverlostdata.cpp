#include "blobstorage_syncer_recoverlostdata.h"
#include "blobstorage_syncer_recoverlostdata_proxy.h"
#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncer_data.h"
#include "syncer_job_actor.h"
#include "syncer_job_task.h"
#include "blobstorage_syncquorum.h"
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_osiris.h>

using namespace NKikimrServices;
using namespace NKikimr::NSync;
using namespace NKikimr::NSyncer;
using namespace std::placeholders;

namespace NKikimr {

    namespace {

        struct TPeerState {
            TActorId ProxyId;
            // We keep NSyncer::TPeerSyncState to facilitate standard Full Sync Job,
            // that we use in Syncer
            NSyncer::TPeerSyncState PeerSyncState;

            void FullSyncDone() {
                ProxyId = TActorId();
            }

            void SetProxyId(const TActorId &aid) {
                ProxyId = aid;
            }

            bool IsFinished() const {
                return ProxyId == TActorId();
            }
        };

    }

    ////////////////////////////////////////////////////////////////////////////
    // State of the full recovery process
    ////////////////////////////////////////////////////////////////////////////
    class TSyncFullRecoverState {
    public:
        TSyncFullRecoverState(const TVDiskIdShort &self,
                              const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top)
            : Neighbors(self, top)
            , QuorumTracker(self, top, false)
            , Sublog(false, self.ToString() + ": ")
        {}

        void FullSyncedWithPeer(const TVDiskIdShort &vdisk) {
            Y_ABORT_UNLESS(Neighbors[vdisk].VDiskIdShort == vdisk);

            Sublog.Log() << "RESPONSE: vdisk# " << vdisk.ToString() << " full synced";

            Neighbors[vdisk].Get().FullSyncDone();
            QuorumTracker.Update(vdisk);
        }

        void RunFullSync(std::function<void(TVDiskInfo<TPeerState>&)> func) {
            for (auto &x : Neighbors) {
                if (!x.MyFailDomain)
                    func(x);
            }
        }

        void NotifyAliveProxies(std::function<void(TVDiskInfo<TPeerState>&)> func) {
            for (auto &x : Neighbors) {
                if (!x.Get().IsFinished()) {
                    // notify proxy
                    func(x);
                }
            }
        }

        bool GotQuorum() const {
            return QuorumTracker.HasQuorum();
        }

    private:
        NSync::TVDiskNeighbors<TPeerState> Neighbors;
        NSync::TQuorumTracker QuorumTracker;
        TSublog<> Sublog;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerRecoverLostDataActor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerRecoverLostDataActor : public TActorBootstrapped<TSyncerRecoverLostDataActor> {
        friend class TActorBootstrapped<TSyncerRecoverLostDataActor>;

        // from protobuf
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;
        using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
        using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;

        TIntrusivePtr<TSyncerContext> SyncerCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TSyncFullRecoverState State;
        const TActorId CommitterId;
        const TActorId NotifyId;
        const TVDiskEternalGuid Guid;
        ui64 DbBirthLsn = 0;
        std::shared_ptr<TSjCtx> JobCtx;

        void Bootstrap(const TActorContext &ctx) {
            // FIXME: RecoverLostData actor MUST remove artefacts of the previous recover try
            // according to cthulhu@: VDisk can send to Node Warden a special message that
            // will lead to VDisk reformat
            // Run full sync recovery and gather quorum
            RunFullSync(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // UTILITIES
        ////////////////////////////////////////////////////////////////////////
        void StopAllRunningProxy(const TActorContext &ctx) {
            auto stopFunc = [&ctx] (TVDiskInfo<TPeerState>& x) {
                Y_ABORT_UNLESS(x.Get().ProxyId);
                ctx.Send(x.Get().ProxyId, new NActors::TEvents::TEvPoisonPill());
            };
            State.NotifyAliveProxies(stopFunc);
        }

        ////////////////////////////////////////////////////////////////////////
        // RUN FULL SYNC
        ////////////////////////////////////////////////////////////////////////
        void RunFullSync(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: START"));

            // run full sync proxy for every VDisk in the group
            auto runProxyForVDisk = [this, &ctx] (TVDiskInfo<TPeerState>& x) {
                auto vd = GInfo->GetVDiskId(x.OrderNumber);
                auto aid = GInfo->GetActorId(x.OrderNumber);
                auto proxyActor = CreateProxyForFullSyncWithPeer(SyncerCtx, x.Get().PeerSyncState,
                                                                 CommitterId, ctx.SelfID,
                                                                 JobCtx, vd, aid);
                auto actorId = ctx.Register(proxyActor);
                x.Get().SetProxyId(actorId);
            };

            State.RunFullSync(runProxyForVDisk);
            Become(&TThis::FullSyncStateFunc);
        }

        void Handle(TEvSyncerFullSyncedWithPeer::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: TEvSyncerFullSyncedWithPeer"));
            auto *msg = ev->Get();
            State.FullSyncedWithPeer(msg->VDiskId);
            if (State.GotQuorum()) {
                StopAllRunningProxy(ctx);
                CallOsiris(ctx);
            }
        }

        STRICT_STFUNC(FullSyncStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerFullSyncedWithPeer, Handle)
            HFunc(TEvVGenerationChange, Handle)
        )

        ////////////////////////////////////////////////////////////////////////
        // CALL OSIRIS
        ////////////////////////////////////////////////////////////////////////
        void CallOsiris(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: CallOsiris"));
            // We must fix DbBirthLsn here, because at this moment we have written some
            // data into Hull Db, but we don't want this data to sync back to other VDisks.
            // This is done by skeleton, because it have access to all Lsn positions (CurrentLsn,
            // AllocLsnForSyncLogLock, etc). Selected Lsn goes back to us via TEvOsirisDone reply.
            ctx.Send(SyncerCtx->SkeletonId, new TEvCallOsiris());
            Become(&TThis::WaitOsirisStateFunc);
        }

        void Handle(TEvOsirisDone::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: TEvOsirisDone"));
            DbBirthLsn = ev->Get()->DbBirthLsn;
            WriteFinalLocally(ctx);
        }

        STRICT_STFUNC(WaitOsirisStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvOsirisDone, Handle)
            IgnoreFunc(TEvSyncerFullSyncedWithPeer)
            HFunc(TEvVGenerationChange, Handle)
        )

        ////////////////////////////////////////////////////////////////////////
        // WRITE FINAL GUID LOCALLY
        ////////////////////////////////////////////////////////////////////////
        void WriteFinalLocally(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: WriteFinalLocally"));
            auto msg = TEvSyncerCommit::LocalFinal(Guid, DbBirthLsn);
            ctx.Send(CommitterId, msg.release());
            Become(&TThis::WriteFinalLocallyStateFunc);
        }

        void HandleFinalLocally(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: TEvSyncerCommitDone"));
            Y_UNUSED(ev);
            Finish(ctx);
        }

        STRICT_STFUNC(WriteFinalLocallyStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerCommitDone, HandleFinalLocally)
            HFunc(TEvVGenerationChange, Handle)
            IgnoreFunc(TEvSyncerFullSyncedWithPeer)
        )

        ////////////////////////////////////////////////////////////////////////
        // WRITE FINAL GUID LOCALLY
        ////////////////////////////////////////////////////////////////////////
        void Finish(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRecoverLostDataActor: FINISH"));

            TLocalSyncerState lss(TLocalVal::Final, Guid, DbBirthLsn);
            ctx.Send(NotifyId, new TEvSyncerLostDataRecovered(lss));
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // HandlePoison
        ////////////////////////////////////////////////////////////////////////
        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            StopAllRunningProxy(ctx);
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // BlobStorage Group reconfiguration
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            // save new Group Info
            auto msg = ev->Get();
            GInfo = msg->NewInfo;
            // recreate JobCtx
            JobCtx = TSjCtx::Create(SyncerCtx, GInfo);
            // reconfigure alive proxies
            auto reconfigureFunc = [&ctx, &msg] (TVDiskInfo<TPeerState>& x) {
                Y_ABORT_UNLESS(x.Get().ProxyId);
                ctx.Send(x.Get().ProxyId, msg->Clone());
            };
            State.NotifyAliveProxies(reconfigureFunc);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_RECOVER_LOST_DATA;
        }

        TSyncerRecoverLostDataActor(const TIntrusivePtr<TSyncerContext> &sc,
                                    const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                    const TActorId &committerId,
                                    const TActorId &notifyId,
                                    TVDiskEternalGuid guid)
            : TActorBootstrapped<TSyncerRecoverLostDataActor>()
            , SyncerCtx(sc)
            , GInfo(info)
            , State(SyncerCtx->VCtx->ShortSelfVDisk, SyncerCtx->VCtx->Top)
            , CommitterId(committerId)
            , NotifyId(notifyId)
            , Guid(guid)
            , JobCtx(TSjCtx::Create(SyncerCtx, GInfo))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateSyncerRecoverLostDataActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateSyncerRecoverLostDataActor(const TIntrusivePtr<TSyncerContext> &sc,
                                             const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                             const TActorId &committerId,
                                             const TActorId &notifyId,
                                             TVDiskEternalGuid guid) {
        return new TSyncerRecoverLostDataActor(sc, info, committerId, notifyId, guid);
    }

} // NKikimr
