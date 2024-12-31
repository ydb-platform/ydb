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

        TVector<T> GetNextBatch() {
            if (Empty()) {
                return {};
            }

            ui32 begin = CurPos;
            ui32 end = Min(begin + BatchSize, static_cast<ui32>(Data.size()));
            CurPos = end;

            return TVector<T>(std::make_move_iterator(Data.begin() + begin), std::make_move_iterator(Data.begin() + end));
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
        TBatchManager(const TActorId& sender, const TActorId& deleter)
            : SenderId(sender)
            , DeleterId(deleter)
        {}
    };

    class TBalancingActor : public TActorBootstrapped<TBalancingActor> {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TLogoBlobsSnapshot::TForwardIterator It;
        TQueueActorMapPtr QueueActorMapPtr;
        THashSet<TVDiskID> ConnectedVDisks;
        THashSet<TVDiskID> GoodStatusVDisks;

        TBatchedQueue<TPartInfo> SendOnMainParts;
        TBatchedQueue<TLogoBlobID> TryDeleteParts;
        std::unordered_map<TLogoBlobID, TVector<TPartInfo>> TryDeletePartsFullData; // if part on main by ingress, but actualy it is not, we could not delete it, so we need to send it on main

        TBatchManager BatchManager;

        TInstant StartTime;
        bool AquiredReplToken = false;

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Init logic
        ///////////////////////////////////////////////////////////////////////////////////////////

        void SendCheckVDisksStatusRequests() {
            for (ui32 i = 0; i < GInfo->GetTotalVDisksNum(); ++i) {
                const auto vdiskId = GInfo->GetVDiskId(i);
                const auto actorId = GInfo->GetActorId(i);
                if (TVDiskIdShort(vdiskId) != Ctx->VCtx->ShortSelfVDisk) {
                    Send(actorId, new TEvBlobStorage::TEvVStatus(vdiskId));
                }
            }
        }

        void Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev) {
            auto msg = ev->Get();
            auto vdiskId = VDiskIDFromVDiskID(msg->Record.GetVDiskID());
            auto status = msg->Record.GetStatus();
            bool replicated = msg->Record.GetReplicated();
            bool isReadOnly = msg->Record.GetIsReadOnly();

            if (status != NKikimrProto::EReplyStatus::OK || !replicated || isReadOnly) {
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB02, VDISKP(Ctx->VCtx, "VDisk is not ready. Stop balancing"),
                    (VDiskId, vdiskId), (Status, NKikimrProto::EReplyStatus_Name(status)), (Replicated, replicated), (ReadOnly, isReadOnly));
                Stop(TDuration::Seconds(10));
                return;
            }

            GoodStatusVDisks.insert(vdiskId);
        }

        bool ReadyToBalance() const {
            return (GoodStatusVDisks.size() + 1 == GInfo->GetTotalVDisksNum()) && (ConnectedVDisks.size() + 1 == GInfo->GetTotalVDisksNum());
        }

        void CollectKeys() {
            if (!ReadyToBalance()) {
                // not all vdisks are connected
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB11, VDISKP(Ctx->VCtx, "Not all vdisks are connected, balancing should work only for full groups"),
                    (ConnectedVDisks, ConnectedVDisks.size()), (GoodStatusVDisks, GoodStatusVDisks.size()), (TotalVDisksInGroup, GInfo->GetTotalVDisksNum()));
                Stop(TDuration::Seconds(10));
                return;
            }

            const auto& top = GInfo->GetTopology();
            TPartsCollectorMerger merger(top.GType);
            THPTimer timer;

            for (ui32 cnt = 0; It.Valid(); It.Next(), ++cnt) {
                if (cnt % 128 == 127 && TDuration::Seconds(timer.Passed()) > Ctx->Cfg.JobGranularity) {
                    // actor should not block the thread for a long time, so we should yield
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Collect keys"), (collected, cnt), (passed, timer.Passed()));
                    Send(SelfId(), new NActors::TEvents::TEvWakeup());
                    return;
                }

                const auto& key = It.GetCurKey().LogoBlobID();

                if (Ctx->Cfg.BalanceOnlyHugeBlobs && !Ctx->HugeBlobCtx->IsHugeBlob(GInfo->Type, key, Ctx->MinREALHugeBlobInBytes)) {
                    // skip non huge blobs
                    continue;
                }

                merger.Clear();
                It.PutToMerger(&merger);

                auto [moveMask, delMask] = merger.Ingress.HandoffParts(&top, Ctx->VCtx->ShortSelfVDisk, key);

                // collect parts to send on main
                if (Ctx->Cfg.EnableSend && SendOnMainParts.Size() < Ctx->Cfg.MaxToSendPerEpoch) {
                    if (auto partsToSend = merger.Ingress.LocalParts(top.GType) & moveMask; !partsToSend.Empty()) {
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
                }

                // collect parts to delete
                if (Ctx->Cfg.EnableDelete && TryDeleteParts.Size() < Ctx->Cfg.MaxToDeletePerEpoch) {
                    if (auto partsToDelete = merger.Ingress.LocalParts(top.GType) & delMask; !partsToDelete.Empty()) {
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
                }

                if (SendOnMainParts.Size() >= Ctx->Cfg.MaxToSendPerEpoch && TryDeleteParts.Size() >= Ctx->Cfg.MaxToDeletePerEpoch) {
                    // reached the limit of parts to send and delete
                    break;
                }
            }

            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB08, VDISKP(Ctx->VCtx, "Keys collected"),
                (SendOnMainParts, SendOnMainParts.Data.size()), (TryDeleteParts, TryDeleteParts.Data.size()));

            // start balancing
            ContinueBalancing();
        }

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Main logic
        ///////////////////////////////////////////////////////////////////////////////////////////

        void ContinueBalancing() {
            Ctx->MonGroup.PlannedToSendOnMain() = SendOnMainParts.Size();
            Ctx->MonGroup.CandidatesToDelete() = TryDeleteParts.Size();

            if (SendOnMainParts.Empty() && TryDeleteParts.Empty()) {
                // no more parts to send or delete
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB03, VDISKP(Ctx->VCtx, "Balancing completed"));
                bool hasSomeWorkForNextEpoch = SendOnMainParts.Size() >= Ctx->Cfg.MaxToSendPerEpoch || TryDeleteParts.Size() >= Ctx->Cfg.MaxToDeletePerEpoch;
                Stop(hasSomeWorkForNextEpoch ? TDuration::Seconds(0) : Ctx->Cfg.TimeToSleepIfNothingToDo);
                return;
            }

            // ask for repl token to continue balancing
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB01, VDISKP(Ctx->VCtx, "Ask repl token to continue balancing"), (SelfId, SelfId()), (PDiskId, Ctx->VDiskCfg->BaseInfo.PDiskId));
            Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(Ctx->VDiskCfg->BaseInfo.PDiskId), NActors::IEventHandle::FlagTrackDelivery);
        }

        void ScheduleJobQuant() {
            Y_DEBUG_ABORT_UNLESS(!AquiredReplToken);
            AquiredReplToken = true;
            Ctx->MonGroup.ReplTokenAquired()++;

            // once repl token received, start balancing - waking up sender and deleter
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB02, VDISKP(Ctx->VCtx, "Schedule job quant"),
                (SendPartsLeft, SendOnMainParts.Size()), (DeletePartsLeft, TryDeleteParts.Size()),
                (ConnectedVDisks, ConnectedVDisks.size()), (TotalVDisks, GInfo->GetTotalVDisksNum()));

            // register sender and deleter actors
            IActor* sender = CreateSenderActor(SelfId(), SendOnMainParts.GetNextBatch(), QueueActorMapPtr, Ctx);
            IActor* deleter = CreateDeleterActor(SelfId(), TryDeleteParts.GetNextBatch(), QueueActorMapPtr, Ctx);

            BatchManager = TBatchManager(
                RegisterWithSameMailbox(sender),
                RegisterWithSameMailbox(deleter)
            );
        }

        void Handle(NActors::TEvents::TEvCompleted::TPtr ev) {
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "TEvCompleted"), (Type, ev->Type));
            BatchManager.Handle(ev);

            if (StartTime + Ctx->Cfg.EpochTimeout < TlsActivationContext->Now()) {
                Ctx->MonGroup.EpochTimeouts()++;
                STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB04, VDISKP(Ctx->VCtx, "Epoch timeout"));
                Stop(TDuration::Seconds(0));
                return;
            }

            if (BatchManager.IsBatchCompleted()) {
                Y_DEBUG_ABORT_UNLESS(AquiredReplToken);
                AquiredReplToken = false;
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
                    Y_VERIFY_DEBUG_S(false, "Part not found in TryDeletePartsFullData");
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
                STLOG(PRI_ERROR, BS_VDISK_BALANCING, BSVB06, VDISKP(Ctx->VCtx, "Ask repl token msg not delivered"), (SelfId, SelfId()), (PDiskId, Ctx->VDiskCfg->BaseInfo.PDiskId));
                ContinueBalancing();
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

        void Stop(TDuration timeoutBeforeNextLaunch) {
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB12, VDISKP(Ctx->VCtx, "Stop balancing"), (SendOnMainParts, SendOnMainParts.Data.size()), (TryDeleteParts, TryDeleteParts.Data.size()), (SecondsBeforeNextLaunch, timeoutBeforeNextLaunch.Seconds()));

            if (AquiredReplToken) {
                Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            }

            Send(BatchManager.SenderId, new NActors::TEvents::TEvPoison);
            Send(BatchManager.DeleterId, new NActors::TEvents::TEvPoison);
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            TlsActivationContext->Schedule(timeoutBeforeNextLaunch, new IEventHandle(Ctx->SkeletonId, SelfId(), new TEvStartBalancing()));
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            // Init events
            cFunc(NActors::TEvents::TEvWakeup::EventType, CollectKeys)
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle)

            // Main logic events
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
            , SendOnMainParts(Ctx->Cfg.BatchSize)
            , TryDeleteParts(Ctx->Cfg.BatchSize)
            , StartTime(TlsActivationContext->Now())
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
            CreateVDisksQueues();
            It.SeekToFirst();
            ++Ctx->MonGroup.BalancingIterations();
            SendCheckVDisksStatusRequests();
            Schedule(TDuration::Seconds(10), new NActors::TEvents::TEvWakeup());
        }
    };

} // NBalancing

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx) {
        return new NBalancing::TBalancingActor(ctx);
    }
} // NKikimr
