#include "balancing_actor.h"
#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>


namespace NKikimr {
namespace NBalancing {

    template<class T>
    struct TBatchedQueue {
        TVector<T> Data;
        ui32 CurPos = 0;
        const ui32 BatchSize = 0;

        TBatchedQueue(ui32 batchSize)
            : BatchSize(batchSize)
        {}

        TConstArrayRef<T> GetNextBatch() {
            if (Empty()) {
                return {};
            }

            ui32 begin = CurPos;
            ui32 end = Min(begin + BatchSize, static_cast<ui32>(Data.size()));
            CurPos = end;
            return TConstArrayRef<T>(Data.data() + begin, end - begin);
        }

        bool Empty() const {
            return CurPos >= Data.size();
        }

        ui32 Size() const {
            return Data.size() - CurPos;
        }
    };

    struct TBatchManager {
        TActorId SenderId;
        TActorId DeleterId;
        bool IsSendCompleted = false;
        bool IsDeleteCompleted = false;

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            if (ev->Sender == SenderId) {
                IsSendCompleted = true;
            } else if (ev->Sender == DeleterId) {
                IsDeleteCompleted = true;
            } else {
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB05, "Unexpected id", (Id, ev->Sender));
            }
        }

        bool IsBatchCompleted() const {
            return IsSendCompleted && IsDeleteCompleted;
        }

        TBatchManager() = default;
        TBatchManager(IActor* sender, IActor* deleter)
            : SenderId(TlsActivationContext->Register(sender))
            , DeleterId(TlsActivationContext->Register(deleter))
        {}
    };

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        THashSet<TVDiskID> ConnectedVDisks;

        TBatchedQueue<TPartInfo> SendOnMainParts;
        TBatchedQueue<TLogoBlobID> TryDeleteParts;

        TBatchManager BatchManager;

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Main logic
        ///////////////////////////////////////////////////////////////////////////////////////////

        void ContinueBalancing() {
            if (SendOnMainParts.Empty() && TryDeleteParts.Empty()) {
                // no more parts to send or delete
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB03, VDISKP(Ctx->VCtx, "Balancing completed"));
                Send(Ctx->SkeletonId, new TEvStartBalancing());
                PassAway();
                return;
            }

            // ask for repl token to continue balancing
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB01, VDISKP(Ctx->VCtx, "Ask repl token to continue balancing"));
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void ScheduleJobQuant() {
            // once repl token received, start balancing - waking up sender and deleter
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB02, VDISKP(Ctx->VCtx, "Schedule job quant"),
                (SendPartsLeft, SendOnMainParts.Size()), (DeletePartsLeft, TryDeleteParts.Size()),
                (ConnectedVDisks, ConnectedVDisks.size()), (TotalVDisks, GInfo->GetTotalVDisksNum()));

            // register sender and deleter actors
            BatchManager = TBatchManager(
                CreateSenderActor(SelfId(), SendOnMainParts.GetNextBatch(), QueueActorMapPtr, Ctx),
                CreateDeleterActor(SelfId(), TryDeleteParts.GetNextBatch(), QueueActorMapPtr, Ctx)
            );
        }

        void CollectKeys() {
            THPTimer timer;

            for (ui32 cnt = 0; It.Valid(); It.Next(), ++cnt) {
                if (cnt % 100 == 99 && TDuration::Seconds(timer.Passed()) > JOB_GRANULARITY) {
                    // actor should not block the thread for a long time, so we should yield
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Collect keys"), (collected, cnt), (passed, timer.Passed()));
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                const auto& top = GInfo->GetTopology();
                const auto& key = It.GetCurKey().LogoBlobID();

                TPartsCollectorMerger merger(top.GType);
                It.PutToMerger(&merger);

                auto [moveMask, delMask] = merger.Ingress.HandoffParts(&top, Ctx->VCtx->ShortSelfVDisk, key);

                if (auto partsToSend = merger.Ingress.LocalParts(top.GType) & moveMask; !partsToSend.Empty()) {
                    // collect parts to send on main
                    for (const auto& [parts, data]: merger.Parts) {
                        if (!(partsToSend & parts).Empty()) {
                            SendOnMainParts.Data.emplace_back(TPartInfo{
                                .Key=It.GetCurKey().LogoBlobID(),
                                .PartsMask=parts,
                                .PartData=data
                            });
                        }
                    }
                }

                if (auto partsToDelete = merger.Ingress.LocalParts(top.GType) & delMask; !partsToDelete.Empty()) {
                    // collect parts to delete
                    for (ui8 partIdx = partsToDelete.FirstPosition(); partIdx < partsToDelete.GetSize(); partIdx = partsToDelete.NextPosition(partIdx)) {
                        TryDeleteParts.Data.emplace_back(TLogoBlobID(It.GetCurKey().LogoBlobID(), partIdx + 1));
                        STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB10, VDISKP(Ctx->VCtx, "Delete"), (LogoBlobId, TryDeleteParts.Data.back().ToString()));
                    }
                }

                merger.Clear();
            }

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB08, VDISKP(Ctx->VCtx, "Keys collected"),
                (SendOnMainParts, SendOnMainParts.Data.size()), (TryDeleteParts, TryDeleteParts.Data.size()));
            Ctx->MonGroup.PlannedToSendOnMain() = SendOnMainParts.Data.size();
            Ctx->MonGroup.CandidatesToDelete() = TryDeleteParts.Data.size();

            // start balancing
            ContinueBalancing();
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            BatchManager.Handle(ev);
            if (BatchManager.IsBatchCompleted()) {
                Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);

                ContinueBalancing();
            }
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
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB06, VDISKP(Ctx->VCtx, "Ask repl token msg not delivered"));
                ScheduleJobQuant();
            }
        }

        void Handle(TEvProxyQueueState::TPtr ev) {
            const TVDiskID& vdiskId = ev->Get()->VDiskId;
            if (ev->Get()->IsConnected) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB07, VDISKP(Ctx->VCtx, "VDisk connected"), (VDiskId, vdiskId.ToString()));
                ConnectedVDisks.insert(vdiskId);
            } else {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB09, VDISKP(Ctx->VCtx, "VDisk disconnected"), (VDiskId, vdiskId.ToString()));
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

            Send(BatchManager.SenderId, msg->Clone());
            Send(BatchManager.DeleterId, msg->Clone());
        }

        void PassAway() override {
            Send(BatchManager.SenderId, new NActors::TEvents::TEvPoison);
            Send(BatchManager.DeleterId, new NActors::TEvents::TEvPoison);
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            // Logic events
            cFunc(NActors::TEvents::TEvWakeup::EventType, CollectKeys)
            cFunc(TEvReplToken::EventType, ScheduleJobQuant)
            hFunc(NActors::TEvents::TEvCompleted, Handle)

            // System events
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
            , SendOnMainParts(BATCH_SIZE)
            , TryDeleteParts(BATCH_SIZE)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
            CreateVDisksQueues();
            It.SeekToFirst();
            ++Ctx->MonGroup.BalancingIterations();
            Schedule(TDuration::Seconds(10), new NActors::TEvents::TEvWakeup());
        }
    };

} // NBalancing

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new NBalancing::TBalancingActor(ctx);
    }
} // NKikimr
