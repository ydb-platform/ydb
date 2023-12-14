#include "balancing_actor.h"
#include "defs.h"
#include "deleter.h"
#include "merger.h"
#include "sender.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>
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
            auto interconnectChannel = static_cast<TInterconnectChannels::EInterconnectChannels>(
                    Ctx->VDiskCfg->ReplInterconnectChannel);
            const TBlobStorageGroupInfo::TTopology& topology = Ctx->GInfo->GetTopology();
            NBackpressure::TQueueClientId replQueueClientId(
                NBackpressure::EQueueClientType::Balancing, topology.GetOrderNumber(Ctx->VCtx->ShortSelfVDisk));

            CreateQueuesForVDisks(*QueueActorMapPtr, SelfId(), Ctx->GInfo, Ctx->VCtx,
                    Ctx->GInfo->GetVDisks(), Ctx->MonGroup.GetGroup(),
                    replQueueClientId, NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
                    "DisksBalancing", interconnectChannel);
        }

        std::pair<TQueue<TPartInfo>, TQueue<TPartInfo>> CollectKeys() {
            TQueue<TPartInfo> sendOnMainParts, tryDeleteParts;

            for (It.SeekToFirst(); It.Valid(); It.Next()) {
                TPartsCollectorMerger merger(Ctx->GInfo->GetTopology().GType);
                It.PutToMerger(&merger);

                auto collectPartsByPredicate = [&](const TVector<ui8>& partIdxs, TQueue<TPartInfo>& queue) {
                    for (ui8 partIdx: partIdxs) {
                        if (!merger.Parts[partIdx - 1].has_value()) {
                            continue;  // something strange
                        }
                        queue.push(TPartInfo{
                            .Key=TLogoBlobID(It.GetCurKey().LogoBlobID(), partIdx),
                            .Ingress=merger.Ingress,
                            .PartData=*merger.Parts[partIdx - 1]
                        });
                    }
                };
                collectPartsByPredicate(PartsToSendOnMain(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress), sendOnMainParts);
                collectPartsByPredicate(PartsToDelete(Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID(), merger.Ingress), tryDeleteParts);

                Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Blob" << " " << It.GetCurKey().LogoBlobID().ToString() << " " << merger.Ingress.ToString(&Ctx->GInfo->GetTopology(), Ctx->VCtx->ShortSelfVDisk, It.GetCurKey().LogoBlobID()) << Endl;

                merger.Clear();
            }
            return {sendOnMainParts, tryDeleteParts};
        }

        void StartBalancing(const TActorContext &ctx) {
            Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Ask repl token " << Endl;
            if (!Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId))) {
                HandleReplToken(ctx);
            }
        }

        void HandleReplToken(const TActorContext &ctx) {
            Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Repl token acquired " << Endl;
            DoJobQuant(ctx);
            Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
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
                Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Balancing completed" << Endl;
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

        void DoJobQuant(const TActorContext &) {
            Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Connected vdisks " << ConnectedVDisks.size() << "/" << Ctx->GInfo->GetTotalVDisksNum() - 1 << Endl;
            // if (ConnectedVDisks.size() == Ctx->GInfo->GetTotalVDisksNum() - 1) {
                Send(SenderId, new NActors::TEvents::TEvWakeup());
                Send(DeleterId, new NActors::TEvents::TEvWakeup());
            // } else {
            //     LOG_WARN_S(ctx, NKikimrServices::BS_SKELETON,
            //         "Balancing actor could not do a job quant, some vdisks are disconnected: "
            //         << ConnectedVDisks.size() << " < " << Ctx->GInfo->GetTotalVDisksNum());
            // }
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvReplToken::EventType, HandleReplToken)
            hFunc(NActors::TEvents::TEvCompleted, Handle)
            hFunc(TEvProxyQueueState, Handle)
            CFunc(NActors::TEvents::TEvWakeup::EventType, StartBalancing)
            CFunc(NActors::TEvents::TEvPoison::EventType, Die)
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
            auto [sendOnMainParts, tryDeleteParts] = CollectKeys();
            Cerr << Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << " " << ctx.Now().MilliSeconds() << " Bootstrap" << ": "
                 << "sendOnMainParts size = " << sendOnMainParts.size() << "; tryDeleteParts size = " << tryDeleteParts.size() << Endl;

            SenderId = ctx.Register(new TSender(SelfId(), std::move(sendOnMainParts), QueueActorMapPtr, Ctx));
            ActiveActors.Insert(SenderId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            DeleterId = ctx.Register(new TDeleter(SelfId(), std::move(sendOnMainParts), QueueActorMapPtr, Ctx));
            ActiveActors.Insert(DeleterId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            Become(&TThis::StateFunc);
            Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
        }
    };

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new TBalancingActor(ctx);
    }
} // NKikimr
