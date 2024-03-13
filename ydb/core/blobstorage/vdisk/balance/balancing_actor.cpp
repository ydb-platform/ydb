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
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        THashSet<TVDiskID> ConnectedVDisks;

        TQueue<TPartInfo> SendOnMainParts;
        TQueue<TPartInfo> TryDeleteParts;

        TActorId SenderId;
        TActorId DeleterId;

        struct TStats {
            bool SendCompleted = false;
            bool DeleteCompleted = false;
        };

        TStats Stats;

        void CreateVDisksQueues() {
            QueueActorMapPtr = std::make_shared<TQueueActorMap>();
            auto interconnectChannel = TInterconnectChannels::EInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA;
            const TBlobStorageGroupInfo::TTopology& topology = Ctx->GInfo->GetTopology();
            NBackpressure::TQueueClientId queueClientId(
                NBackpressure::EQueueClientType::Balancing, topology.GetOrderNumber(Ctx->VCtx->ShortSelfVDisk));

            CreateQueuesForVDisks(*QueueActorMapPtr, SelfId(), Ctx->GInfo, Ctx->VCtx,
                    Ctx->GInfo->GetVDisks(), Ctx->MonGroup.GetGroup(),
                    queueClientId, NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
                    "DisksBalancing", interconnectChannel);
        }

        constexpr static TDuration JOB_GRANULARITY = TDuration::MilliSeconds(1);

        void CollectKeys() {
            THPTimer timer;

            for (ui32 cnt = 0; It.Valid(); It.Next(), ++cnt) {
                if (cnt % 1000 == 999 && TDuration::Seconds(timer.Passed()) > JOB_GRANULARITY) {
                    BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Collected " << cnt << " keys");
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                TPartsCollectorMerger merger(Ctx->GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                for (ui8 partId: PartIdsToSendOnMain(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
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
                        << SendOnMainParts.back().Ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, SendOnMainParts.back().Key));
                }

                for (ui8 partId: PartIdsToDelete(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    TryDeleteParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partId),
                        .Ingress=merger.Ingress,
                    });
                    BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Delete: " << TryDeleteParts.back().Key.ToString()
                            << " " << TryDeleteParts.back().Ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, TryDeleteParts.back().Key));
                }
                merger.Clear();
            }

            FinishCollecting();
        }

        void FinishCollecting() {
            auto ctx = *TlsActivationContext;

            BLOG_D(Ctx->VCtx->VDiskLogPrefix << ctx.Now().MilliSeconds() << " Bootstrap" << ": "
                << "sendOnMainParts size = " << SendOnMainParts.size() << "; tryDeleteParts size = " << TryDeleteParts.size());

            SenderId = ctx.Register(CreateSenderActor(SelfId(), std::move(SendOnMainParts), QueueActorMapPtr, Ctx));
            DeleterId = ctx.Register(CreateDeleterActor(SelfId(), std::move(TryDeleteParts), QueueActorMapPtr, Ctx));

            Become(&TThis::StateFunc);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }

        void StartBalancing() {
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Ask repl token");
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void Handle(NActors::TEvents::TEvUndelivered::TPtr ev) {
            if (ev.Get()->Type == TEvReplToken::EventType) {
                BLOG_W(Ctx->VCtx->VDiskLogPrefix << "Repl token not delivered");
                HandleReplToken();
            }
        }

        void HandleReplToken() {
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Repl token acquired");
            DoJobQuant();
            Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            switch (ev->Get()->Id) {
                case SENDER_ID:
                    Stats.SendCompleted = true;
                    break;
                case DELETER_ID:
                    Stats.DeleteCompleted = true;
                    break;
                default:
                    Y_ABORT("Unexpected id");
            }
            if (Stats.SendCompleted && Stats.DeleteCompleted) {
                BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Balancing completed");
                Send(Ctx->SkeletonId, new TEvStartBalancing());
                PassAway();
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
            Ctx->GInfo = msg->NewInfo;
        }

        void DoJobQuant() {
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Connected vdisks " << ConnectedVDisks.size() << "/" << Ctx->GInfo->GetTotalVDisksNum() - 1);
            Send(SenderId, new NActors::TEvents::TEvWakeup());
            Send(DeleterId, new NActors::TEvents::TEvWakeup());
        }

        void PassAway() override {
            Send(SenderId, new NActors::TEvents::TEvPoison);
            Send(DeleterId, new NActors::TEvents::TEvPoison);
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(CollectKeysState,
            cFunc(NActors::TEvents::TEvWakeup::EventType, CollectKeys)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)

            // BSQueue specific events
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, Handle)
        );

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, StartBalancing)
            hFunc(NActors::TEvents::TEvUndelivered, Handle)
            cFunc(TEvReplToken::EventType, HandleReplToken)
            hFunc(NActors::TEvents::TEvCompleted, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)

            // BSQueue specific events
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, Handle)
        );

    public:
        TBalancingActor(std::shared_ptr<TBalancingCtx> &ctx)
            : TActorBootstrapped<TBalancingActor>()
            , Ctx(ctx)
            , It(Ctx->Snap.HullCtx, &Ctx->Snap.LogoBlobsSnap)
        {
        }

        void Bootstrap() {
            CreateVDisksQueues();
            It.SeekToFirst();
            Become(&TThis::CollectKeysState);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());            
        }
    };

} // NBalancing

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new NBalancing::TBalancingActor(ctx);
    }
} // NKikimr
