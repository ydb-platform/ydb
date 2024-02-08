#include "syncer_job_actor.h"
#include "syncer_job_task.h"
#include "syncer_context.h"
#include "blobstorage_syncer_localwriter.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubisrunner.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>
#include <ydb/core/base/interconnect_channels.h>
#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncer;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerJob
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerJob : public TActorBootstrapped<TSyncerJob> {
    protected:
        // from protobuf
        using ESyncStatus = NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus;
        using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;
        using TBase = TActorBootstrapped<TSyncerJob>;

        TIntrusivePtr<TSyncerContext> SyncerCtx;
        std::unique_ptr<TSyncerJobTask> Task;
        const ui32 NodeId;
        const TActorId NotifyId;
        const ui64 JobId;       // just unique job id for log readability

        friend class TActorBootstrapped<TSyncerJob>;

        // handle outcome
        void Handle(TSjOutcome &&outcome, const TActorContext &ctx) {
            if (outcome.Ev) {
                SendOutcomeMsg(std::move(outcome.Ev), std::move(outcome.To), ctx);
            }

            if (outcome.ActorActivity) {
                if (outcome.RunInBatchPool) {
                    RunInBatchPool(ctx, outcome.ActorActivity.release());
                } else {
                    ctx.Register(outcome.ActorActivity.release());
                }
            }

            if (outcome.Die) {
                // if full sync, notify AnubisRunner about it
                if (Task->IsFullRecoveryTask()) {
                    TVDiskIdShort vd = Task->VDiskId;
                    ctx.Send(SyncerCtx->AnubisRunnerId, new TEvFullSyncedWith(vd));
                }
                ctx.Send(NotifyId, new TEvSyncerJobDone(std::move(Task)));
                Die(ctx);
            }
        }

        // function for sending a message
        void SendOutcomeMsg(std::unique_ptr<IEventBase> &&ev, TActorId &&to, const TActorContext &ctx) {
            Y_ABORT_UNLESS(ev && to != TActorId());
            // subscribe on Interconnect Session/Message tracking
            ui32 flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession;
            const auto channel = TInterconnectChannels::IC_BLOBSTORAGE_SYNCER;
            flags = IEventHandle::MakeFlags(channel, flags);

            ctx.Send(to, ev.release(), flags);
        }

        // overridden Die (unsubsribe from Message/Session tracking)
        void Die(const TActorContext &ctx) override {
            // unsubscribe on session when die
            ctx.Send(TActivationContext::InterconnectProxy(NodeId),
                     new TEvents::TEvUnsubscribe);
            TBase::Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerJob::Handle(TEvNodeDisconnected): msg# %s",
                            ev->Get()->ToString().data()));

            TSjOutcome outcome = Task->Terminate(TSyncStatusVal::DroppedConnection);
            Handle(std::move(outcome), ctx);
        }

        void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "TSyncerJob::Handle(TEvUndelivered): msg# %s",
                            ev->Get()->ToString().data()));

            TSjOutcome outcome = Task->Terminate(TSyncStatusVal::DroppedConnection);
            Handle(std::move(outcome), ctx);
        }

        void Handle(TEvBlobStorage::TEvVSyncFullResult::TPtr &ev, const TActorContext &ctx) {
            TSjOutcome outcome = Task->Handle(ev, SelfId());
            Handle(std::move(outcome), ctx);
        }

        void Handle(TEvBlobStorage::TEvVSyncResult::TPtr &ev, const TActorContext &ctx) {
            TSjOutcome outcome = Task->Handle(ev, SelfId());
            Handle(std::move(outcome), ctx);
        }

        void Handle(TEvLocalSyncDataResult::TPtr &ev, const TActorContext &ctx) {
            TSjOutcome outcome = Task->Handle(ev);
            Handle(std::move(outcome), ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                VDISKP(SyncerCtx->VCtx->VDiskLogPrefix, "TSyncerJob: job timed out"));

            TSjOutcome outcome = Task->Terminate(TSyncStatusVal::Timeout);
            Handle(std::move(outcome), ctx);
        }

        void Bootstrap(const TActorContext &ctx) {
            // don't run sync job for too long
            if (const auto timeout = SyncerCtx->Config->SyncJobTimeout; timeout != TDuration::Max()) {
                ctx.Schedule(timeout, new TEvents::TEvWakeup());
            }

            // initiate requests
            TSjOutcome outcome = Task->NextRequest();
            Y_ABORT_UNLESS(!outcome.Die && outcome.Ev); // can't die in this case and must have event
            Handle(std::move(outcome), ctx);

            // state function
            Become(&TThis::StateFunc);
        }

        // use it for debug purposes
        void HeavyDump(const TActorContext &ctx, const TString &data) const {
            // record handlers
            auto blobHandler = [&] (const NSyncLog::TLogoBlobRec *rec) {
                LOG_ERROR(ctx, BS_SYNCER,
                          VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                            "TSyncerJob::Sync: JobId# %" PRIu64 " FullRecover# %u"
                                " id# %s ignress# %s",
                                JobId, unsigned(Task->IsFullRecoveryTask()),
                                rec->LogoBlobID().ToString().data(),
                                rec->Ingress.ToString(SyncerCtx->VCtx->Top.get(),
                                                       SyncerCtx->VCtx->ShortSelfVDisk,
                                                       rec->LogoBlobID()).data()));
            };
            auto blockHandler = [&] (const NSyncLog::TBlockRec *rec) {
                LOG_ERROR(ctx, BS_SYNCER,
                          VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                            "TSyncerJob::Sync: JobId# %" PRIu64 " FullRecover# %u rec# %s",
                                JobId, unsigned(Task->IsFullRecoveryTask()), rec->ToString().data()));
            };
            auto barrierHandler = [&] (const NSyncLog::TBarrierRec *rec) {
                LOG_ERROR(ctx, BS_SYNCER,
                          VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                            "TSyncerJob::Sync: JobId# %" PRIu64 " FullRecover# %u rec# %s",
                                JobId, unsigned(Task->IsFullRecoveryTask()), rec->ToString().data()));
            };
            auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2 *rec) {
                LOG_ERROR(ctx, BS_SYNCER, VDISKP(SyncerCtx->VCtx->VDiskLogPrefix, "TSyncerJob::Sync: JobId# %" PRIu64
                    " FullRecover# %u rec# %s", JobId, unsigned(Task->IsFullRecoveryTask()), rec->ToString().data()));
            };

            NSyncLog::TFragmentReader fragment(data);
            fragment.ForEach(blobHandler, blockHandler, barrierHandler, blockHandlerV2);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_JOB;
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvBlobStorage::TEvVSyncFullResult, Handle)
            HFunc(TEvBlobStorage::TEvVSyncResult, Handle)
            HFunc(TEvLocalSyncDataResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
            IgnoreFunc(TEvInterconnect::TEvNodeConnected)
            HFunc(TEvents::TEvUndelivered, Handle)
            IgnoreFunc(TEvBlobStorage::TEvVWindowChange) // ignore TEvVWindowChange
        )

    public:
        TSyncerJob(const TIntrusivePtr<TSyncerContext> &sc,
                   std::unique_ptr<TSyncerJobTask> task,
                   const TActorId &notifyId)
            : TActorBootstrapped<TSyncerJob>()
            , SyncerCtx(sc)
            , Task(std::move(task))
            , NodeId(Task->ServiceId.NodeId())
            , NotifyId(notifyId)
            , JobId(TAppData::RandomProvider->GenRand64())
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerJob CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncerJob(const TIntrusivePtr<TSyncerContext> &sc,
                            std::unique_ptr<TSyncerJobTask> task,
                            const TActorId &notifyId) {
        return new TSyncerJob(sc, std::move(task), notifyId);
    }

} // NKikimr
