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
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Ask repl token");
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void ScheduleJobQuant() {
            // once repl token received, start balancing - waking up sender and deleter
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Connected vdisks " << ConnectedVDisks.size() << "/" << GInfo->GetTotalVDisksNum() - 1);
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
                BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Balancing completed");
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
                    BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Collected " << cnt << " keys");
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                TPartsCollectorMerger merger(GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                // collect parts to send on main
                for (ui8 partId: PartIdsToSendOnMain(GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    if (!merger.Parts[partId - 1].has_value()) {
                        BLOG_W(Ctx->VCtx->VDiskLogPrefix << "not found part " << (ui32)partId << " for " << It.GetCurKey().LogoBlobID().ToString());
                        continue;  // something strange
                    }
                    SendOnMainParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partId),
                        .Ingress=merger.Ingress,
                        .PartData=*merger.Parts[partId - 1]
                    });
                    BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Send on main: " << SendOnMainParts.back().Key.ToString() << " "
                        << SendOnMainParts.back().Ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, SendOnMainParts.back().Key));
                }

                // collect parts to delete
                for (ui8 partId: PartIdsToDelete(GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    TryDeleteParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partId),
                        .Ingress=merger.Ingress,
                    });
                    BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Delete: " << TryDeleteParts.back().Key.ToString()
                            << " " << TryDeleteParts.back().Ingress.ToString(&GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, TryDeleteParts.back().Key));
                }
                merger.Clear();
            }

            BLOG_D(Ctx->VCtx->VDiskLogPrefix << " Bootstrap" << ": "
                << "sendOnMainParts size = " << SendOnMainParts.size() << "; tryDeleteParts size = " << TryDeleteParts.size());

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
                BLOG_W(Ctx->VCtx->VDiskLogPrefix << "Aske repl token msg not delivered");
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
