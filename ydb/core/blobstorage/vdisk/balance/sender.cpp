#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>

#include <ydb/core/util/stlog.h>


namespace NKikimr {
namespace NBalancing {
namespace {
    struct TPart {
        TLogoBlobID Key;
        NMatrix::TVectorType PartsMask;
        TVector<TRope> PartsData;
    };

    class TReader {
    private:
        TPDiskCtxPtr PDiskCtx;
        TConstArrayRef<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        const TBlobStorageGroupType GType;
        NMonGroup::TBalancingGroup& MonGroup;

        TVector<TPart> Result;
        ui32 Responses = 0;
    public:

        TReader(TPDiskCtxPtr pDiskCtx, TConstArrayRef<TPartInfo> parts, TReplQuoter::TPtr replPDiskReadQuoter, TBlobStorageGroupType gType, NMonGroup::TBalancingGroup& monGroup)
            : PDiskCtx(pDiskCtx)
            , Parts(parts)
            , Quoter(replPDiskReadQuoter)
            , GType(gType)
            , MonGroup(monGroup)
            , Result(parts.size())
        {}

        void SendReadRequests(const TActorId& selfId) {
            for (ui32 i = 0; i < Parts.size(); ++i) {
                const auto& item = Parts[i];
                Result[i] = TPart{.Key=item.Key, .PartsMask=item.PartsMask};
                std::visit(TOverloaded{
                    [&](const TRope& data) {
                        // part is already in memory, no need to read it from disk
                        Y_DEBUG_ABORT_UNLESS(item.PartsMask.CountBits() == 1);
                        Result[i].PartsData = {data};
                        ++Responses;
                    },
                    [&](const TDiskPart& diskPart) {
                        auto ev = std::make_unique<NPDisk::TEvChunkRead>(
                            PDiskCtx->Dsk->Owner,
                            PDiskCtx->Dsk->OwnerRound,
                            diskPart.ChunkIdx,
                            diskPart.Offset,
                            diskPart.Size,
                            NPriRead::HullLow,
                            reinterpret_cast<void*>(i)
                        );

                        TReplQuoter::QuoteMessage(
                            Quoter,
                            std::make_unique<IEventHandle>(PDiskCtx->PDiskId, selfId, ev.release()),
                            diskPart.Size
                        );
                        MonGroup.ReadFromHandoffBytes() += diskPart.Size;
                    }
                }, item.PartData);
            }
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            ++Responses;
            auto *msg = ev->Get();
            if (msg->Status != NKikimrProto::EReplyStatus::OK) {
                return;
            }
            ui64 i = reinterpret_cast<ui64>(msg->Cookie);
            const auto& key = Result[i].Key;
            auto data = TRope(msg->Data.ToString());
            auto localParts = Result[i].PartsMask;
            auto diskBlob = TDiskBlob(&data, localParts, GType, key);
            ui32 readSize = 0;

            for (ui8 partIdx = localParts.FirstPosition(); partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx)) {
                TRope result;
                result = diskBlob.GetPart(partIdx, &result);
                readSize += result.size();
                Result[i].PartsData.emplace_back(std::move(result));
            }

            MonGroup.ReadFromHandoffResponseBytes() += readSize;
        }

        ui32 GetPartsSize() const {
            return Parts.size();
        }

        ui32 GetResponses() const {
            return Responses;
        }

        bool IsDone() const {
            return Responses == Parts.size();
        }

        TVector<TPart>& GetResult() {
            return Result;
        }
    };

    class TPartsSender {
    private:
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQueueActorMapPtr QueueActorMapPtr;

        ui32 RequestsSent = 0;
        ui32 Responses = 0;
    public:

        TPartsSender(
            std::shared_ptr<TBalancingCtx> ctx,
            TIntrusivePtr<TBlobStorageGroupInfo> gInfo,
            TQueueActorMapPtr queueActorMapPtr
        )
            : Ctx(ctx)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
        {}

        void SendRequest(const TVDiskIdShort& vDiskId, const TActorId& selfId, IEventBase* ev, ui32 dataSize) {
            auto& queue = (*QueueActorMapPtr)[vDiskId];
            TReplQuoter::QuoteMessage(
                Ctx->VCtx->ReplNodeRequestQuoter,
                std::make_unique<IEventHandle>(queue, selfId, ev),
                dataSize
            );
            RequestsSent++;
            Ctx->MonGroup.SentOnMainBytes() += dataSize;
        }

        void SendPartsOnMain(const TActorId& selfId, TVector<TPart>& parts) {
            THashMap<TVDiskID, std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vDiskToEv;
            for (auto& part: parts) {
                auto localParts = part.PartsMask;
                for (ui8 partIdx = localParts.FirstPosition(), i = 0; partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx), ++i) {
                    auto key = TLogoBlobID(part.Key, partIdx + 1);
                    auto& data = part.PartsData[i];
                    auto vDiskId = GetMainReplicaVDiskId(*GInfo, key);

                    if (Ctx->HugeBlobCtx->IsHugeBlob(GInfo->GetTopology().GType, part.Key, Ctx->MinREALHugeBlobInBytes)) {
                        auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(
                            key, data, vDiskId,
                            true, nullptr,
                            TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob
                        );
                        SendRequest(TVDiskIdShort(vDiskId), selfId, ev.release(), data.size());
                    } else {
                        STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB11, VDISKP(Ctx->VCtx, "Add in multiput"), (LogoBlobId, key.ToString()),
                            (To, GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))), (DataSize, data.size()));

                        auto& ev = vDiskToEv[vDiskId];
                        if (!ev) {
                            ev = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob, true, nullptr);
                        }

                        ev->AddVPut(key, TRcBuf(data), nullptr, {}, NWilson::TTraceId());
                    }
                }
            }

            for (auto& [vDiskId, ev]: vDiskToEv) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB12, VDISKP(Ctx->VCtx, "Send multiput"), (VDisk, vDiskId.ToString()));

                ui32 blobsSize = 0;
                for (const auto& item: ev->Record.GetItems()) {
                    blobsSize += item.GetBuffer().size();
                }

                SendRequest(TVDiskIdShort(vDiskId), selfId, ev.release(), blobsSize);
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            ++Responses;
            if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB13, VDISKP(Ctx->VCtx, "Put failed"), (Msg, ev->Get()->ToString()));
            } else {
                ++Ctx->MonGroup.SentOnMain();
                Ctx->MonGroup.SentOnMainWithResponseBytes() += GInfo->GetTopology().GType.PartSize(LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID()));
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB14, VDISKP(Ctx->VCtx, "Put done"), (Msg, ev->Get()->ToString()));
            }
        }

        void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr ev) {
            ++Responses;
            const auto& items = ev->Get()->Record.GetItems();
            for (const auto& item: items) {
                if (item.GetStatus() != NKikimrProto::OK) {
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB15, VDISKP(Ctx->VCtx, "MultiPut failed"), (Key, LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()));
                    continue;
                }
                ++Ctx->MonGroup.SentOnMain();
                Ctx->MonGroup.SentOnMainWithResponseBytes() += GInfo->GetTopology().GType.PartSize(LogoBlobIDFromLogoBlobID(item.GetBlobID()));
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB16, VDISKP(Ctx->VCtx, "MultiPut done"), (Key, LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()));
            }
        }

        bool IsDone() const {
            return Responses == RequestsSent;
        }

    };

    class TSender : public TActorBootstrapped<TSender> {
        TActorId NotifyId;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TReader Reader;
        TPartsSender Sender;

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  StateRead
        ///////////////////////////////////////////////////////////////////////////////////////////

        void ReadPartsFromDisk() {
            Become(&TThis::StateRead);

            if (Reader.GetPartsSize() == 0) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB10, VDISKP(Ctx->VCtx, "Nothing to read. PassAway"));
                PassAway();
                return;
            }

            Reader.SendReadRequests(SelfId());
            if (Reader.IsDone()) {
                SendPartsOnMain();
                return;
            }

            Schedule(TDuration::Seconds(15), new NActors::TEvents::TEvWakeup(READ_TIMEOUT_TAG)); // read timeout
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            Reader.Handle(ev);
            if (Reader.IsDone()) {
                SendPartsOnMain();
            }
        }

        void TimeoutRead(NActors::TEvents::TEvWakeup::TPtr ev) {
            if (ev->Get()->Tag != READ_TIMEOUT_TAG) {
                return;
            }
            STLOG(PRI_INFO, BS_VDISK_BALANCING, BSVB17, VDISKP(Ctx->VCtx, "TimeoutRead"), (Requests, Reader.GetPartsSize()),  (Responses, Reader.GetResponses()));
            SendPartsOnMain();
        }

        STRICT_STFUNC(StateRead,
            hFunc(NPDisk::TEvChunkReadResult, Handle)
            hFunc(NActors::TEvents::TEvWakeup, TimeoutRead)

            hFunc(TEvVGenerationChange, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        );

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  StateSend
        ///////////////////////////////////////////////////////////////////////////////////////////

        void SendPartsOnMain() {
            Become(&TThis::StateSend);

            if (Reader.GetResult().empty()) {
                STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB18, VDISKP(Ctx->VCtx, "Nothing to send. PassAway"));
                PassAway();
                return;
            }

            Sender.SendPartsOnMain(SelfId(), Reader.GetResult());

            Schedule(TDuration::Seconds(15), new NActors::TEvents::TEvWakeup(SEND_TIMEOUT_TAG)); // send timeout
        }

        template<class TEvPutResult>
        void HandlePutResult(TEvPutResult ev) {
            Sender.Handle(ev);
            if (Sender.IsDone()) {
                PassAway();
            }
        }

        void TimeoutSend(NActors::TEvents::TEvWakeup::TPtr ev) {
            if (ev->Get()->Tag != SEND_TIMEOUT_TAG) {
                return;
            }
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB19, VDISKP(Ctx->VCtx, "TimeoutSend"));
            PassAway();
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateSend,
            hFunc(NActors::TEvents::TEvWakeup, TimeoutSend)
            hFunc(TEvBlobStorage::TEvVPutResult, HandlePutResult)
            hFunc(TEvBlobStorage::TEvVMultiPutResult, HandlePutResult)

            hFunc(TEvVGenerationChange, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        );

        ///////////////////////////////////////////////////////////////////////////////////////////
        //  Helper functions
        ///////////////////////////////////////////////////////////////////////////////////////////

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

    public:
        TSender(
            TActorId notifyId,
            TConstArrayRef<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , Reader(Ctx->PDiskCtx, parts, ctx->VCtx->ReplPDiskReadQuoter, GInfo->GetTopology().GType, Ctx->MonGroup)
            , Sender(ctx, GInfo, queueActorMapPtr)
        {}

        void Bootstrap() {
            ReadPartsFromDisk();
        }
    };
}

IActor* CreateSenderActor(
    TActorId notifyId,
    TConstArrayRef<TPartInfo> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TSender(notifyId, parts, queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
