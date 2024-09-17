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
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB05, "Unexpected actor id", (Id, ev->Sender));
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
        std::unordered_map<TLogoBlobID, TVector<TPartInfo>> TryDeletePartsFullData; // if part on main by ingress, but actualy it is not, we could not delete it, so we need to send it on main

        TBatchManager BatchManager;

        TInstant StartTime;

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Main logic
        ///////////////////////////////////////////////////////////////////////////////////////////

        void ContinueBalancing() {
            if (SendOnMainParts.Empty() && TryDeleteParts.Empty()) {
                // no more parts to send or delete
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB03, VDISKP(Ctx->VCtx, "Balancing completed"));
                PassAway();
                return;
            }

            // ask for repl token to continue balancing
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB01, VDISKP(Ctx->VCtx, "Ask repl token to continue balancing"));
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void ScheduleJobQuant() {
            Ctx->MonGroup.ReplTokenAquired()++;
            Ctx->MonGroup.PlannedToSendOnMain() = SendOnMainParts.Data.size();
            Ctx->MonGroup.CandidatesToDelete() = TryDeleteParts.Data.size();

            // once repl token received, start balancing - waking up sender and deleter
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB02, VDISKP(Ctx->VCtx, "Schedule job quant"),
                (SendPartsLeft, SendOnMainParts.Size()), (DeletePartsLeft, TryDeleteParts.Size()),
                (ConnectedVDisks, ConnectedVDisks.size()), (TotalVDisks, GInfo->GetTotalVDisksNum()));

            // register sender and deleter actors
            BatchManager = TBatchManager(
                CreateSenderActor(SelfId(), SendOnMainParts.GetNextBatch(), QueueActorMapPtr, Ctx),
                CreateDeleterActor(SelfId(), TryDeleteParts.GetNextBatch(), QueueActorMapPtr, Ctx)
            );
        }

        void CollectKeys() {
            if (ConnectedVDisks.size() + 1 != GInfo->GetTotalVDisksNum()) {
                // not all vdisks are connected
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB11, VDISKP(Ctx->VCtx, "Not all vdisks are connected, balancing should work only for full groups"),
                    (ConnectedVDisks, ConnectedVDisks.size()), (TotalVDisksInGroup, GInfo->GetTotalVDisksNum()));
                PassAway();
                return;
            }

            THPTimer timer;

            for (ui32 cnt = 0; It.Valid(); It.Next(), ++cnt) {
                if (cnt % 100 == 99 && TDuration::Seconds(timer.Passed()) > JOB_GRANULARITY) {
                    // actor should not block the thread for a long time, so we should yield
                    // STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Collect keys"), (collected, cnt), (passed, timer.Passed()));
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                const auto& top = GInfo->GetTopology();
                const auto& key = It.GetCurKey().LogoBlobID();

                TPartsCollectorMerger merger(top.GType);
                It.PutToMerger(&merger);

                auto [moveMask, delMask] = merger.Ingress.HandoffParts(&top, Ctx->VCtx->ShortSelfVDisk, key);

                if (auto partsToSend = merger.Ingress.LocalParts(top.GType) & moveMask; !partsToSend.Empty() && SendOnMainParts.Size() < MAX_TO_SEND_PER_EPOCH) {
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

                if (auto partsToDelete = merger.Ingress.LocalParts(top.GType) & delMask; !partsToDelete.Empty() && TryDeleteParts.Size() < MAX_TO_DELETE_PER_EPOCH) {
                    // collect parts to delete
                    auto key = It.GetCurKey().LogoBlobID();
                    for (ui8 partIdx = partsToDelete.FirstPosition(); partIdx < partsToDelete.GetSize(); partIdx = partsToDelete.NextPosition(partIdx)) {
                        TryDeleteParts.Data.emplace_back(TLogoBlobID(key, partIdx + 1));
                        STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB10, VDISKP(Ctx->VCtx, "Delete"), (LogoBlobId, TryDeleteParts.Data.back().ToString()));
                    }

                    for (const auto& [parts, data]: merger.Parts) {
                        if (!(partsToDelete & parts).Empty()) {
                            TryDeletePartsFullData[key].emplace_back(TPartInfo{
                                .Key=key, .PartsMask=parts, .PartData=data
                            });
                        }
                    }
                }

                merger.Clear();

                if (SendOnMainParts.Size() >= MAX_TO_SEND_PER_EPOCH && TryDeleteParts.Size() >= MAX_TO_DELETE_PER_EPOCH) {
                    // reached the limit of parts to send and delete
                    break;
                }
            }

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB08, VDISKP(Ctx->VCtx, "Keys collected"),
                (SendOnMainParts, SendOnMainParts.Data.size()), (TryDeleteParts, TryDeleteParts.Data.size()));

            // start balancing
            ContinueBalancing();
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            BatchManager.Handle(ev);

            if (StartTime + EPOCH_TIMEOUT < TlsActivationContext->Now()) {
                Ctx->MonGroup.EpochTimeouts()++;
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Epoch timeout"));
                PassAway();
            }

            if (BatchManager.IsBatchCompleted()) {
                Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);

                ContinueBalancing();
            }
        }

        void Handle(TEvBalancingSendPartsOnMain::TPtr ev) {
            Ctx->MonGroup.OnMainByIngressButNotRealy() += ev->Get()->Ids.size();
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB05, VDISKP(Ctx->VCtx, "Received from deleter TEvBalancingSendPartsOnMain"), (Parts, ev->Get()->Ids.size()));
            for (const auto& id: ev->Get()->Ids) {
                if (auto it = TryDeletePartsFullData.find(TLogoBlobID(id, 0)); it != TryDeletePartsFullData.end()) {
                    for (const auto& part: it->second) {
                        if (part.PartsMask.Get(id.PartId() - 1)) {
                            SendOnMainParts.Data.push_back(part);
                            break;
                        }
                    }
                } else {
                    Y_DEBUG_ABORT_S("Part not found in TryDeletePartsFullData");
                }
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
            Send(Ctx->SkeletonId, new TEvStartBalancing());
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            // Logic events
            cFunc(NActors::TEvents::TEvWakeup::EventType, CollectKeys)
            cFunc(TEvReplToken::EventType, ScheduleJobQuant)
            hFunc(NActors::TEvents::TEvCompleted, Handle)
            hFunc(TEvBalancingSendPartsOnMain, Handle)

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
            , StartTime(TlsActivationContext->Now())
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
