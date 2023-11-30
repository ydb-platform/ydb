#include "blobstorage_syncer_recoverlostdata_proxy.h"
#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncer_data.h"
#include "blobstorage_syncquorum.h"
#include "syncer_job_actor.h"
#include "syncer_job_task.h"
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_osiris.h>

using namespace NKikimrServices;
using namespace NKikimr::NSync;
using namespace NKikimr::NSyncer;
using namespace std::placeholders;

namespace NKikimr {


    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerRLDWakeup
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerRLDWakeup
        : public TEventLocal<TEvSyncerRLDWakeup, TEvBlobStorage::EvSyncerRLDWakeup>
    {
        // Our task we working on
        std::unique_ptr<TSyncerJobTask> Task;

        TEvSyncerRLDWakeup(std::unique_ptr<TSyncerJobTask> task)
            : Task(std::move(task))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerRLDFullSyncProxyActor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerRLDFullSyncProxyActor : public TActorBootstrapped<TSyncerRLDFullSyncProxyActor> {
        friend class TActorBootstrapped<TSyncerRLDFullSyncProxyActor>;

        TIntrusivePtr<TSyncerContext> SyncerCtx;
        const NSyncer::TPeerSyncState PeerSyncState;
        const TActorId CommitterId;
        const TActorId NotifyId;
        TActiveActors ActiveActors;
        std::shared_ptr<TSjCtx> JobCtx;
        // Target VDiskId and ActorId are reconfigurable
        TVDiskID TargetVDiskId;
        TActorId TargetActorId;

        void CreateAndRunTask(const TActorContext &ctx) {
            // create task
            auto task = std::make_unique<TSyncerJobTask>(TSyncerJobTask::EFullRecover, TargetVDiskId, TargetActorId,
                PeerSyncState, JobCtx);
            // run task
            const TActorId aid = ctx.Register(CreateSyncerJob(SyncerCtx, std::move(task), ctx.SelfID));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            // state func
            Become(&TThis::WaitForSyncStateFunc);
        }

        void Bootstrap(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): START",
                            TargetVDiskId.ToString().data()));

            // run job
            CreateAndRunTask(ctx);
        }

        void Handle(TEvSyncerJobDone::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): TEvSyncerJobDone; Task# %s",
                            TargetVDiskId.ToString().data(), ev->Get()->Task->ToString().data()));
            ActiveActors.Erase(ev->Sender);
            std::unique_ptr<TSyncerJobTask> task = std::move(ev->Get()->Task);
            auto syncStatus = task->GetCurrent().LastSyncStatus;
            if (!TPeerSyncState::Good(syncStatus)) {
                RerunTaskAfterTimeout(ctx);
            } else {
                Commit(ctx, std::move(task));
            }
        }

        STRICT_STFUNC(WaitForSyncStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerJobDone, Handle)
            HFunc(TEvVGenerationChange, Handle)
        )

        ////////////////////////////////////////////////////////////////////////
        // WAIT FOR TIMEOUT
        ////////////////////////////////////////////////////////////////////////
        void RerunTaskAfterTimeout(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): RerunTaskAfterTimeout",
                            TargetVDiskId.ToString().data()));
            auto timeout = SyncerCtx->Config->SyncerRLDRetryTimeout;
            ctx.Schedule(timeout, new TEvSyncerRLDWakeup(nullptr));
            // state func
            Become(&TThis::WaitForTimeoutStateFunc);
        }

        void Handle(TEvSyncerRLDWakeup::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            // run job again
            CreateAndRunTask(ctx);
        }

        STRICT_STFUNC(WaitForTimeoutStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerRLDWakeup, Handle)
            IgnoreFunc(TEvSyncerJobDone)
            HFunc(TEvVGenerationChange, Handle)
        )

        ////////////////////////////////////////////////////////////////////////
        // COMMIT
        ////////////////////////////////////////////////////////////////////////
        void Commit(const TActorContext &ctx, std::unique_ptr<TSyncerJobTask> task) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): Commit",
                            TargetVDiskId.ToString().data()));
            auto msg = TEvSyncerCommit::Remote(task->VDiskId, task->GetCurrent());
            ctx.Send(CommitterId, msg.release());
            Become(&TThis::WaitForCommitStateFunc);
        }

        void Handle(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): FINISH",
                           TargetVDiskId.ToString().data()));
            ctx.Send(NotifyId, new TEvSyncerFullSyncedWithPeer(TargetVDiskId));
            Die(ctx);
        }

        STRICT_STFUNC(WaitForCommitStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(TEvVGenerationChange, Handle)
        )

        ////////////////////////////////////////////////////////////////////////
        // HandlePoison
        ////////////////////////////////////////////////////////////////////////
        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                     VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerRLDFullSyncProxyActor(%s): PoisonPill",
                           TargetVDiskId.ToString().data()));
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // BlobStorage Group reconfiguration
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            auto msg = ev->Get();
            // reconfigure target VDiskId and ActorId
            auto shortTargetVDiskId = TVDiskIdShort(TargetVDiskId);
            TargetVDiskId = msg->NewInfo->GetVDiskId(shortTargetVDiskId);
            TargetActorId = msg->NewInfo->GetActorId(shortTargetVDiskId);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_RECOVER_LOST_DATA;
        }

        TSyncerRLDFullSyncProxyActor(const TIntrusivePtr<TSyncerContext> &sc,
                                     const TPeerSyncState& peerSyncState,
                                     const TActorId &committerId,
                                     const TActorId &notifyId,
                                     const std::shared_ptr<TSjCtx> &jobCtx,
                                     const TVDiskID &targetVDiskId,
                                     const TActorId &targetActorId)
            : TActorBootstrapped<TSyncerRLDFullSyncProxyActor>()
            , SyncerCtx(sc)
            , PeerSyncState(peerSyncState)
            , CommitterId(committerId)
            , NotifyId(notifyId)
            , JobCtx(jobCtx)
            , TargetVDiskId(targetVDiskId)
            , TargetActorId(targetActorId)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerRLDFullSyncProxyActor CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateProxyForFullSyncWithPeer(const TIntrusivePtr<TSyncerContext> &sc,
                                           const TPeerSyncState& peerSyncState,
                                           const TActorId &committerId,
                                           const TActorId &notifyId,
                                           const std::shared_ptr<TSjCtx> &jobCtx,
                                           const TVDiskID &targetVDiskId,
                                           const TActorId &targetActorId) {
        return new TSyncerRLDFullSyncProxyActor(sc, peerSyncState, committerId, notifyId,
                                                jobCtx, targetVDiskId, targetActorId);
    }

} // NKikimr
