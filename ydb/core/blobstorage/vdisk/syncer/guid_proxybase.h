#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr {


    ////////////////////////////////////////////////////////////////////////////
    // TVDiskGuidProxyBase
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskGuidProxyBase : public TActorBootstrapped<TVDiskGuidProxyBase> {
    protected:
        friend class TActorBootstrapped<TVDiskGuidProxyBase>;

        TIntrusivePtr<TVDiskContext> VCtx;
        // these fields below are reconfigurable
        TVDiskID SelfVDiskId;
        TVDiskID TargetVDiskId;
        TActorId TargetServiceId;
        // notify this actor when done
        const TActorId NotifyId;
        const TDuration RetryPeriod = TDuration::MilliSeconds(50);
        TTrivialLogThrottler LogThrottler = { TDuration::Minutes(1) };


        // override these functions to get required functionality
        virtual std::unique_ptr<TEvBlobStorage::TEvVSyncGuid> GenerateRequest() = 0;
        virtual void HandleReply(const TActorContext &ctx,
                                 const NKikimrBlobStorage::TEvVSyncGuidResult &record) = 0;


        void Die(const TActorContext &ctx) override {
            // unsubscribe on session when die
            auto nodeId = TargetServiceId.NodeId();
            ctx.Send(TActivationContext::InterconnectProxy(nodeId),
                     new TEvents::TEvUnsubscribe);
            TActor::Die(ctx);
        }

        void SendRequestAndWaitForResponse(const TActorContext &ctx) {
            // we don't use cookie, because in general any response is OK

            // track delivery and subscribe on session
            ctx.Send(TargetServiceId, GenerateRequest().release(),
                     IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

            Become(&TThis::StateFunc);
        }

        void Bootstrap(const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidProxyBase: START: targetVDiskId# %s",
                            TargetVDiskId.ToString().data()));

            SendRequestAndWaitForResponse(ctx);
        }

        void Handle(TEvBlobStorage::TEvVSyncGuidResult::TPtr& ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidProxyBase::Handle(TEvVSyncGuidResult): msg# %s",
                            ev->Get()->ToString().data()));

            const NKikimrBlobStorage::TEvVSyncGuidResult &record = ev->Get()->Record;
            // check status
            if (record.GetStatus() != NKikimrProto::OK) {
                // log result of guid recovery
                auto pri = NActors::NLog::PRI_ERROR;
                if (record.GetStatus() == NKikimrProto::NOTREADY || record.GetStatus() == NKikimrProto::RACE)
                    pri = NActors::NLog::PRI_INFO;
                LOG_LOG_THROTTLE(LogThrottler, ctx, pri, NKikimrServices::BS_SYNCER,
                                 VDISKP(VCtx->VDiskLogPrefix,
                                    "TVDiskGuidProxyBase::Handle(TEvVSyncGuidResult): NOT OK: msg# %s",
                                    ev->Get()->ToString().data()));

                // retry in case of error
                Become(&TThis::StateSleep, ctx, RetryPeriod, new TEvents::TEvWakeup());
                return;
            }

            TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
            if (!SelfVDiskId.SameGroupAndGeneration(fromVDisk)) {
                // In case of race, we retry. The idea is either target VDisk restarts in the correct
                // blobstorage group, or we restart or reconfigure with correct blobstorage group settings
                LOG_WARN(ctx, NKikimrServices::BS_SYNCER,
                         VDISKP(VCtx->VDiskLogPrefix,
                            "TVDiskGuidProxyBase::Handle(TEvVSyncGuidResult): RACE: msg# %s",
                            ev->Get()->ToString().data()));

                Become(&TThis::StateSleep, ctx, RetryPeriod, new TEvents::TEvWakeup());
                return;
            }

            Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
            HandleReply(ctx, record);
            Die(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidProxyBase::Handle(TEvNodeDisconnected): msg# %s",
                            ev->Get()->ToString().data()));

            Become(&TThis::StateSleep, ctx, RetryPeriod, new TEvents::TEvWakeup());
        }

        void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, NKikimrServices::BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidProxyBase::Handle(TEvUndelivered): msg# %s",
                            ev->Get()->ToString().data()));

            Become(&TThis::StateSleep, ctx, RetryPeriod, new TEvents::TEvWakeup());
        }

        void HandleWakeup(const TActorContext &ctx) {
            SendRequestAndWaitForResponse(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        // BlobStorage Group reconfiguration
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
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
            HFunc(TEvents::TEvUndelivered, Handle)
            HFunc(TEvBlobStorage::TEvVSyncGuidResult, Handle)
            IgnoreFunc(TEvInterconnect::TEvNodeConnected)
            HFunc(TEvVGenerationChange, Handle)
        )

        STRICT_STFUNC(StateSleep,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            IgnoreFunc(TEvInterconnect::TEvNodeDisconnected)
            IgnoreFunc(TEvents::TEvUndelivered)
            IgnoreFunc(TEvBlobStorage::TEvVSyncGuidResult)
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup)
            IgnoreFunc(TEvInterconnect::TEvNodeConnected)
            HFunc(TEvVGenerationChange, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNC_WRITE_VDISK_GUID_PROXY;
        }

        TVDiskGuidProxyBase(TIntrusivePtr<TVDiskContext> vctx,
                            const TVDiskID &selfVDiskId,
                            const TVDiskID &targetVDiskId,
                            const TActorId &targetServiceId,
                            const TActorId &notifyId)
            : TActorBootstrapped<TVDiskGuidProxyBase>()
            , VCtx(vctx)
            , SelfVDiskId(selfVDiskId)
            , TargetVDiskId(targetVDiskId)
            , TargetServiceId(targetServiceId)
            , NotifyId(notifyId)
        {}
    };

} // NKikimr
