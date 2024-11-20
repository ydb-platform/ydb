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
        TQueue<TLogoBlobID> TryDeleteParts;

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
                    Stats.SendCompleted = Stats.SendPartsLeft == 0;
                    Stats.DeleteCompleted = Stats.DeletePartsLeft == 0;
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

                const auto& top = GInfo->GetTopology();
                const auto& key = It.GetCurKey().LogoBlobID();

                TPartsCollectorMerger merger(top.GType);
                It.PutToMerger(&merger);

                auto [moveMask, delMask] = merger.Ingress.HandoffParts(&top, Ctx->VCtx->ShortSelfVDisk, key);
                auto partsToSend = merger.Ingress.LocalParts(top.GType) & moveMask;
                auto partsToDelete = merger.Ingress.LocalParts(top.GType) & delMask;

                // collect parts to send on main
                for (const auto& [parts, data]: merger.Parts) {
                    if (!(partsToSend & parts).Empty()) {
                        SendOnMainParts.push(TPartInfo{
                            .Key=It.GetCurKey().LogoBlobID(),
                            .PartsMask=parts,
                            .PartData=data
                        });
                    }
                }

                // collect parts to delete
                for (ui8 partIdx = partsToDelete.FirstPosition(); partIdx < partsToDelete.GetSize(); partIdx = partsToDelete.NextPosition(partIdx)) {
                    TryDeleteParts.push(TLogoBlobID(It.GetCurKey().LogoBlobID(), partIdx + 1));
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB07, VDISKP(Ctx->VCtx, "Delete"), (LogoBlobId, TryDeleteParts.back().ToString()));
                }

                merger.Clear();
            }

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB08, VDISKP(Ctx->VCtx, "Keys collected"),
                (SendOnMainParts, SendOnMainParts.size()), (TryDeleteParts, TryDeleteParts.size()));
            Ctx->MonGroup.PlannedToSendOnMain() = SendOnMainParts.size();
            Ctx->MonGroup.CandidatesToDelete() = TryDeleteParts.size();

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
                    "DisksBalancing", interconnectChannel, false);
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
