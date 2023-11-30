#include "guid_propagator.h"
#include "guid_proxywrite.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/util/activeactors.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerGuidPropagator
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerGuidPropagator : public TActorBootstrapped<TSyncerGuidPropagator> {
        friend class TActorBootstrapped<TSyncerGuidPropagator>;

        TIntrusivePtr<TVDiskContext> VCtx;
        // vdisk/actor ids are reconfigurable, they are changed during group reconfiguration
        TVDiskID SelfVDiskId;
        TVDiskID TargetVDiskId;
        TActorId TargetServiceId;
        // const values we propagate
        const NKikimrBlobStorage::TSyncGuidInfo::EState State;
        const TVDiskEternalGuid Guid;
        // actors we borned
        TActorId WriterId;
        TActiveActors ActiveActors;
        const TDuration WaitPeriod = TDuration::Seconds(10);


        void RunWriter(const TActorContext &ctx) {
            WriterId = ctx.Register(CreateProxyForWritingVDiskGuid(VCtx,
                                                                   SelfVDiskId,
                                                                   TargetVDiskId,
                                                                   TargetServiceId,
                                                                   ctx.SelfID,
                                                                   State,
                                                                   Guid));
            ActiveActors.Insert(WriterId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            TThis::Become(&TThis::StateFuncWrite);
        }

        void Wait(const TActorContext &ctx) {
            ctx.Schedule(WaitPeriod, new TEvents::TEvWakeup());
            TThis::Become(&TThis::StateFuncWait);
        }

        ////////////////////////////////////////////////////////////////////////
        // Handlers
        ////////////////////////////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            RunWriter(ctx);
        }

        void Handle(TEvVDiskGuidWritten::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            WriterId = TActorId();
            auto *msg = ev->Get();
            Y_ABORT_UNLESS(msg->State == State &&
                     msg->Guid == Guid &&
                     TVDiskIdShort(msg->VDiskId) == TVDiskIdShort(TargetVDiskId));
            // wait for WaitPeriod
            Wait(ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            RunWriter(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // BlobStorage Group Configuration
        ////////////////////////////////////////////////////////////////////////
        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);

            // extract info
            auto *msg = ev->Get();
            TIntrusivePtr<TBlobStorageGroupInfo> &info = msg->NewInfo;

            // update self
            auto shortSelf = TVDiskIdShort(SelfVDiskId);
            SelfVDiskId = info->GetVDiskId(shortSelf);

            // update target
            auto shortTarget = TVDiskIdShort(TargetVDiskId);
            TargetVDiskId = info->GetVDiskId(shortTarget);
            TargetServiceId = info->GetActorId(shortTarget);

            // reconfigure writer
            if (WriterId) {
                ctx.Send(WriterId, ev->Get()->Clone());
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // State Functions
        ////////////////////////////////////////////////////////////////////////
        STRICT_STFUNC(StateFuncWrite,
            HFunc(TEvVDiskGuidWritten, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvVGenerationChange, Handle)
        )

        STRICT_STFUNC(StateFuncWait,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvVGenerationChange, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_GUID_PROPAGATOR;
        }

        TSyncerGuidPropagator(TIntrusivePtr<TVDiskContext> vctx,
                              const TVDiskID &selfVDiskId,
                              const TVDiskID &targetVDiskId,
                              const TActorId &targetServiceId,
                              NKikimrBlobStorage::TSyncGuidInfo::EState state,
                              TVDiskEternalGuid guid)
            : TActorBootstrapped<TSyncerGuidPropagator>()
            , VCtx(std::move(vctx))
            , SelfVDiskId(selfVDiskId)
            , TargetVDiskId(targetVDiskId)
            , TargetServiceId(targetServiceId)
            , State(state)
            , Guid(guid)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // SYNCER GUID PROPAGATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncerGuidPropagator(TIntrusivePtr<TVDiskContext> vctx,
                                       const TVDiskID &selfVDiskId,
                                       const TVDiskID &targetVDiskId,
                                       const TActorId &targetServiceId,
                                       NKikimrBlobStorage::TSyncGuidInfo::EState state,
                                       TVDiskEternalGuid guid) {
        return new TSyncerGuidPropagator(std::move(vctx), selfVDiskId, targetVDiskId, targetServiceId,
                                         state, guid);
    }


} // NKikimr
