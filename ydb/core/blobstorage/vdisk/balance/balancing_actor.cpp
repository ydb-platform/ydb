#include "balancing_actor.h"
#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>


namespace NKikimr {
namespace NBalancing {

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        THashSet<TVDiskID> ConnectedVDisks;

        TQueue<TPartInfo> SendOnMainParts;
        TQueue<TPartInfo> TryDeleteParts;

        TActorId SenderId;
        TActorId DeleterId;

        struct TStats {
            bool SendCompleted = false;
            ui32 SendPartsLeft = 0;
            bool DeleteCompleted = false;
            ui32 DeletePartsLeft = 0;
        };

        TStats Stats;


        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Main logic
        ///////////////////////////////////////////////////////////////////////////////////////////

        void ContinueBalancing() {
            // ask for repl token to continue balancing
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB01, VDISKP(Ctx->VCtx, "Ask repl token to continue balancing"));
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void ScheduleJobQuant() {
            // once repl token received, start balancing - waking up sender and deleter
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB02, VDISKP(Ctx->VCtx, "Schedule job quant"),
                (SendPartsLeft, Stats.SendPartsLeft), (DeletePartsLeft, Stats.DeletePartsLeft),
                (ConnectedVDisks, ConnectedVDisks.size()), (TotalVDisks, GInfo->GetTotalVDisksNum()));
            Send(SenderId, new NActors::TEvents::TEvWakeup());
            Send(DeleterId, new NActors::TEvents::TEvWakeup());
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            // sender or deleter completed job quant
            switch (ev->Get()->Id) {
                case SENDER_ID:
                    Stats.SendCompleted = true;
                    Stats.SendPartsLeft = ev->Get()->Status;
                    break;
                case DELETER_ID:
                    Stats.DeleteCompleted = true;
                    Stats.DeletePartsLeft = ev->Get()->Status;
                    break;
                default:
                    Y_ABORT("Unexpected id");
            }

            if (Stats.SendCompleted && Stats.DeleteCompleted) {
                // balancing job quant completed, release token
                Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);

                if (Stats.SendPartsLeft != 0 || Stats.DeletePartsLeft != 0) {
                    // sender or deleter has not finished yet
                    ContinueBalancing();
                    return;
                }

                // balancing completed
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB03, VDISKP(Ctx->VCtx, "Balancing completed"));
                Send(Ctx->SkeletonId, new TEvStartBalancing());
                PassAway();
            }
        }

        constexpr static TDuration JOB_GRANULARITY = TDuration::MilliSeconds(1);

        void CollectKeys() {
            THPTimer timer;

            for (ui32 cnt = 0; It.Valid(); It.Next(), ++cnt) {
                if (cnt % 1000 == 999 && TDuration::Seconds(timer.Passed()) > JOB_GRANULARITY) {
                    // actor should not block the thread for a long time, so we should yield
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Collect keys"), (collected, cnt));
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                TPartsCollectorMerger merger(GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                // collect parts to send on main
                for (ui8 partId: PartIdsToSendOnMain(GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    if (!merger.Parts[partId - 1].has_value()) {
                        STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB05, VDISKP(Ctx->VCtx, "not found part"), (partId, (ui32)partId), (logoBlobId, It.GetCurKey().LogoBlobID()));
                        continue;  // something strange
                    }
                    SendOnMainParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partId),
                        .Ingress=merger.Ingress,
                        .PartData=*merger.Parts[partId - 1]
                    });
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB06, VDISKP(Ctx->VCtx, "Send on main"), (LogoBlobId, SendOnMainParts.back().Key.ToString()),
                        (Ingress, SendOnMainParts.back().Ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, SendOnMainParts.back().Key)));
                }

                // collect parts to delete
                for (ui8 partId: PartIdsToDelete(GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    TryDeleteParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partId),
                        .Ingress=merger.Ingress,
                    });
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB07, VDISKP(Ctx->VCtx, "Delete"), (LogoBlobId, TryDeleteParts.back().Key.ToString()),
                        (Ingress, TryDeleteParts.back().Ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, TryDeleteParts.back().Key)));
                }
                merger.Clear();
            }

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB08, VDISKP(Ctx->VCtx, "Keys collected"),
                (SendOnMainParts, SendOnMainParts.size()), (TryDeleteParts, TryDeleteParts.size()));

            // register sender and deleter actors
            SenderId = TlsActivationContext->Register(CreateSenderActor(SelfId(), std::move(SendOnMainParts), QueueActorMapPtr, Ctx));
            DeleterId = TlsActivationContext->Register(CreateDeleterActor(SelfId(), std::move(TryDeleteParts), QueueActorMapPtr, Ctx));

            // start balancing
            ContinueBalancing();
        }


        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Helper functions
        ///////////////////////////////////////////////////////////////////////////////////////////

        void CreateVDisksQueues() {
            QueueActorMapPtr = std::make_shared<TQueueActorMap>();
            auto interconnectChannel = TInterconnectChannels::EInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA;
            const TBlobStorageGroupInfo::TTopology& topology = GInfo->GetTopology();
            NBackpressure::TQueueClientId queueClientId(
                NBackpressure::EQueueClientType::Balancing, topology.GetOrderNumber(Ctx->VCtx->ShortSelfVDisk));

            CreateQueuesForVDisks(*QueueActorMapPtr, SelfId(), GInfo, Ctx->VCtx,
                    GInfo->GetVDisks(), Ctx->MonGroup.GetGroup(),
                    queueClientId, NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
                    "DisksBalancing", interconnectChannel);
        }

        void Handle(NActors::TEvents::TEvUndelivered::TPtr ev) {
            if (ev.Get()->Type == TEvReplToken::EventType) {
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB09, VDISKP(Ctx->VCtx, "Aske repl token msg not delivered"));
                ScheduleJobQuant();
            }
        }

        void Handle(TEvProxyQueueState::TPtr ev) {
            const TVDiskID& vdiskId = ev->Get()->VDiskId;
            if (ev->Get()->IsConnected) {
                ConnectedVDisks.insert(vdiskId);
            } else {
                ConnectedVDisks.erase(vdiskId);
            }
        }

        void Handle(TEvVGenerationChange::TPtr ev) {
            // forward message to queue actors
            TEvVGenerationChange *msg = ev->Get();
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, msg->Clone());
            }
            GInfo = msg->NewInfo;

            Send(SenderId, msg->Clone());
            Send(DeleterId, msg->Clone());
        }

        void PassAway() override {
            Send(SenderId, new NActors::TEvents::TEvPoison);
            Send(DeleterId, new NActors::TEvents::TEvPoison);
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, CollectKeys)
            cFunc(TEvReplToken::EventType, ScheduleJobQuant)
            hFunc(NActors::TEvents::TEvCompleted, Handle)

            hFunc(NActors::TEvents::TEvUndelivered, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)

            // BSQueue specific events
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, Handle)
        );

    public:
        TBalancingActor(std::shared_ptr<TBalancingCtx> &ctx)
            : TActorBootstrapped<TBalancingActor>()
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , It(Ctx->Snap.HullCtx, &Ctx->Snap.LogoBlobsSnap)
        {
        }

        void Bootstrap() {
            CreateVDisksQueues();
            It.SeekToFirst();
            Become(&TThis::StateFunc);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());            
        }
    };

} // NBalancing

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new NBalancing::TBalancingActor(ctx);
    }
} // NKikimr
