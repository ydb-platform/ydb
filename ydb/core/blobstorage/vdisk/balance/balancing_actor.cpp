#include "balancing_actor.h"
#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>


namespace NKikimr {

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        TActiveActors ActiveActors;
        THashSet<TVDiskID> ConnectedVDisks;

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

        std::pair<TQueue<TPartInfo>, TQueue<TPartInfo>> CollectKeys(const TActorContext &ctx) {
            TQueue<TPartInfo> sendOnMainParts, tryDeleteParts;

            for (It.SeekToFirst(); It.Valid(); It.Next()) {
                TPartsCollectorMerger merger(Ctx->GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                for (ui8 partIdx: PartsToSendOnMain(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    if (!merger.Parts[partIdx - 1].has_value()) {
                        LOG_WARN_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                            Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                            << "not found part " << (ui32)partIdx << " for " << It.GetCurKey().LogoBlobID().ToString());
                        continue;  // something strange
                    }
                    sendOnMainParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partIdx),
                        .Ingress=merger.Ingress,
                        .PartData=*merger.Parts[partIdx - 1]
                    });
                    LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                            Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                            << "Send on main: " << sendOnMainParts.back().Key.ToString() << " " << sendOnMainParts.back().Ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, sendOnMainParts.back().Key));
                }

                for (ui8 partIdx: PartsToDelete(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress)) {
                    tryDeleteParts.push(TPartInfo{
                        .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partIdx),
                        .Ingress=merger.Ingress,
                    });
                    LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                            Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                            << "Delete: " << tryDeleteParts.back().Key.ToString() << " " << tryDeleteParts.back().Ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, tryDeleteParts.back().Key));
                }
                merger.Clear();
            }
            return {sendOnMainParts, tryDeleteParts};
        }

        void StartBalancing(const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Ask repl token");
            if (!Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId))) {
                HandleReplToken(ctx);
            }
        }

        void HandleReplToken(const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Repl token acquired");
            DoJobQuant(ctx);
            Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev, const TActorContext &ctx) {
            switch (ev->Get()->Id) {
                case SENDER_ID: {
                    Stats.SendCompleted = true;
                    break;
                }
                case DELETER_ID: {
                    Stats.DeleteCompleted = true;
                    break;
                }
                default:
                    Y_ABORT("Unexpected id");
            }
            if (Stats.SendCompleted && Stats.DeleteCompleted) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                            Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Balancing completed");
                Send(Ctx->SkeletonId, new TEvStartBalancing());
                Send(SelfId(), new NActors::TEvents::TEvPoison);
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
            Cerr << "TEvVGenerationChange" << Endl;
            // forward message to queue actors
            TEvVGenerationChange *msg = ev->Get();
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, msg->Clone());
            }
            Ctx->GInfo = msg->NewInfo;
        }

        void DoJobQuant(const TActorContext& ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Connected vdisks " << ConnectedVDisks.size() << "/" << Ctx->GInfo->GetTotalVDisksNum() - 1);
            Send(SenderId, new NActors::TEvents::TEvWakeup());
            Send(DeleterId, new NActors::TEvents::TEvWakeup());
        }

        void PassAway() override {
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvReplToken::EventType, HandleReplToken)
            HFunc(NActors::TEvents::TEvCompleted, Handle)
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, Handle)
            CFunc(NActors::TEvents::TEvWakeup::EventType, StartBalancing)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        );

    public:
        TBalancingActor(std::shared_ptr<TBalancingCtx> &ctx)
            : TActorBootstrapped<TBalancingActor>()
            , Ctx(ctx)
            , It(Ctx->Snap.HullCtx, &Ctx->Snap.LogoBlobsSnap)
        {
        }

        void Bootstrap(const TActorContext &ctx) {
            CreateVDisksQueues();
            auto [sendOnMainParts, tryDeleteParts] = CollectKeys(ctx);
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                << ctx.Now().MilliSeconds() << " Bootstrap" << ": "
                << "sendOnMainParts size = " << sendOnMainParts.size() << "; tryDeleteParts size = " << tryDeleteParts.size());

            SenderId = ctx.Register(CreateSenderActor(SelfId(), std::move(sendOnMainParts), QueueActorMapPtr, Ctx));
            ActiveActors.Insert(SenderId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            DeleterId = ctx.Register(CreateDeleterActor(SelfId(), std::move(tryDeleteParts), QueueActorMapPtr, Ctx));
            ActiveActors.Insert(DeleterId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            Become(&TThis::StateFunc);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }
    };

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new TBalancingActor(ctx);
    }
} // NKikimr
