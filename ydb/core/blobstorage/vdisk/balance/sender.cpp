#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>

#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_BALANCING


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
        TVector<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        NMonitoring::TDynamicCounters::TCounterPtr QuoterThrottledCounter;
        const TBlobStorageGroupType GType;
        NMonGroup::TBalancingGroup& MonGroup;
        TVector<TPart> Result;
        ui32 Responses = 0;
    public:

        TReader(TPDiskCtxPtr pDiskCtx, TVector<TPartInfo>&& parts, TReplQuoter::TPtr replPDiskReadQuoter, NMonitoring::TDynamicCounters::TCounterPtr quoterThrottledCounter, TBlobStorageGroupType gType, NMonGroup::TBalancingGroup& monGroup)
            : PDiskCtx(pDiskCtx)
            , Parts(std::move(parts))
            , Quoter(replPDiskReadQuoter)
            , QuoterThrottledCounter(quoterThrottledCounter)
            , GType(gType)
            , MonGroup(monGroup)
            , Result(Parts.size())
        {}

        void SendReadRequests(const TActorId& selfId) {
            for (ui32 i = 0; i < Parts.size(); ++i) {
                auto& item = Parts[i];
                Result[i] = TPart{
                    .Key = item.Key,
                    .PartsMask = item.PartsMask,
                };
                std::visit(TOverloaded{
                    [&](TRope&& data) {
                        // part is already in memory, no need to read it from disk
                        Y_DEBUG_ABORT_UNLESS(item.PartsMask.CountBits() == 1);
                        Result[i].PartsData.emplace_back(std::move(data));
                        ++Responses;
                    },
                    [&](TDiskPart&& diskPart) {
                        auto ev = std::make_unique<NPDisk::TEvChunkRead>(
                            PDiskCtx->Dsk->Owner,
                            PDiskCtx->Dsk->OwnerRound,
                            diskPart.ChunkIdx,
                            diskPart.Offset,
                            diskPart.Size,
                            NPriRead::HullLow,
                            reinterpret_cast<void*>(i)
                        );

                        if (item.IsHugeBlob) {
                            Y_ABORT_UNLESS(item.PartsMask.CountBits() == 1);
                            ev->BlobId = TLogoBlobID(item.Key, item.PartsMask.FirstPosition() + 1);
                        }

                        TReplQuoter::QuoteMessage(
                            Quoter,
                            std::make_unique<IEventHandle>(PDiskCtx->PDiskId, selfId, ev.release()),
                            diskPart.Size,
                            0,
                            QuoterThrottledCounter
                        );
                        MonGroup.ReadFromHandoffBytes() += diskPart.Size;
                    }
                }, std::move(item.PartData));
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

            Result[i].PartsData.reserve(localParts.CountBits());

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
        NMonGroup::TReplGroup& ReplMonGroup;

        ui32 RequestsSent = 0;
        ui32 Responses = 0;
    public:

        TPartsSender(
            std::shared_ptr<TBalancingCtx> ctx,
            TIntrusivePtr<TBlobStorageGroupInfo> gInfo,
            TQueueActorMapPtr queueActorMapPtr,
            NMonGroup::TReplGroup& replMonGroup
        )
            : Ctx(ctx)
            , GInfo(gInfo)
            , QueueActorMapPtr(queueActorMapPtr)
            , ReplMonGroup(replMonGroup)
        {}

        void SendRequest(const TVDiskIdShort& vDiskId, const TActorId& selfId, IEventBase* ev, ui32 dataSize) {
            auto& queue = (*QueueActorMapPtr)[vDiskId];
            TReplQuoter::QuoteMessage(
                Ctx->VCtx->ReplNodeRequestQuoter,
                std::make_unique<IEventHandle>(queue, selfId, ev),
                dataSize,
                0,
                ReplMonGroup.ReplNodeRequestThrottledMicrosecondsPtr()
            );
            RequestsSent++;
            Ctx->MonGroup.SentOnMainBytes() += dataSize;
        }

        void SendPartsOnMain(const TActorId& selfId, TVector<TPart>& parts) {
            THashMap<TVDiskID, std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> vDiskToEv;
            for (auto& part: parts) {
                if (part.PartsData.empty()) {
                    continue;
                }
                auto localParts = part.PartsMask;
                for (ui8 partIdx = localParts.FirstPosition(), i = 0; partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx), ++i) {
                    auto key = TLogoBlobID(part.Key, partIdx + 1);
                    auto&& data = std::move(part.PartsData[i]);
                    size_t dataSize = data.size();
                    auto vDiskId = GetMainReplicaVDiskId(*GInfo, key);

                    if (Ctx->HugeBlobCtx->IsHugeBlob(GInfo->GetTopology().GType, part.Key, Ctx->MinHugeBlobInBytes)) {
                        // TODO(alexvru): checksumming here
                        auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(
                            key, std::move(data), vDiskId,
                            true, nullptr,
                            TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob,
                            false
                        );
                        SendRequest(TVDiskIdShort(vDiskId), selfId, ev.release(), dataSize);
                    } else {
                        YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Add in multiput"),
                            {"Marker", "BSVB11"},
                            {"LogoBlobId", key.ToString()},
                            {"To", GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))},
                            {"DataSize", dataSize});

                        auto& ev = vDiskToEv[vDiskId];
                        if (!ev) {
                            ev = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob, true, nullptr);
                        }

                        // TODO(alexvru): checksumming here
                        ev->AddVPut(key, TRcBuf(std::move(data)), nullptr, false, true, false, {}, NWilson::TTraceId(), false);
                    }
                }
            }

            for (auto& [vDiskId, ev]: vDiskToEv) {
                YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Send multiput"),
                    {"Marker", "BSVB12"},
                    {"VDisk", vDiskId.ToString()});
                const size_t bytes = ev->GetBufferBytes();
                SendRequest(TVDiskIdShort(vDiskId), selfId, ev.release(), bytes);
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            ++Responses;
            if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
                YDB_LOG_WARN(VDISKP(Ctx->VCtx, "Put failed"),
                    {"Marker", "BSVB13"},
                    {"Msg", ev->Get()->ToString()});
            } else {
                ++Ctx->MonGroup.SentOnMain();
                Ctx->MonGroup.SentOnMainWithResponseBytes() += GInfo->GetTopology().GType.PartSize(LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID()));
                YDB_LOG_INFO(VDISKP(Ctx->VCtx, "Put done"),
                    {"Marker", "BSVB14"},
                    {"Msg", ev->Get()->ToString()});
            }
        }

        void Handle(TEvBlobStorage::TEvVMultiPutResult::TPtr ev) {
            ++Responses;
            auto rec = ev->Get()->Record;
            if (rec.GetStatus()  != NKikimrProto::OK) {
                YDB_LOG_WARN(VDISKP(Ctx->VCtx, "MultiPut failed"),
                    {"Marker", "BSVB33"},
                    {"Msg", ev->Get()->ToString()});
                return;
            }

            const auto& items = ev->Get()->Record.GetItems();
            for (const auto& item: items) {
                if (item.GetStatus() != NKikimrProto::OK) {
                    YDB_LOG_WARN(VDISKP(Ctx->VCtx, "MultiPut item failed"),
                        {"Marker", "BSVB15"},
                        {"Key", LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()},
                        {"Status", NKikimrProto::EReplyStatus_Name(item.GetStatus())},
                        {"Error", item.GetErrorReason()});
                    continue;
                }
                ++Ctx->MonGroup.SentOnMain();
                Ctx->MonGroup.SentOnMainWithResponseBytes() += GInfo->GetTopology().GType.PartSize(LogoBlobIDFromLogoBlobID(item.GetBlobID()));
                YDB_LOG_INFO(VDISKP(Ctx->VCtx, "MultiPut done"),
                    {"Marker", "BSVB16"},
                    {"Key", LogoBlobIDFromLogoBlobID(item.GetBlobID()).ToString()});
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

            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "ReadPartsFromDisk"),
                {"Marker", "BSVB29"},
                {"Parts", Reader.GetPartsSize()});

            if (Reader.GetPartsSize() == 0) {
                YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Nothing to read. PassAway"),
                    {"Marker", "BSVB10"});
                PassAway();
                return;
            }

            Reader.SendReadRequests(SelfId());
            if (Reader.IsDone()) {
                SendPartsOnMain();
                return;
            }

            Schedule(Ctx->Cfg.ReadBatchTimeout, new NActors::TEvents::TEvWakeup(READ_TIMEOUT_TAG)); // read timeout
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
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "ReadFromHandoffBatchTimeout"),
                {"Marker", "BSVB17"},
                {"Requests", Reader.GetPartsSize()},
                {"Responses", Reader.GetResponses()});
            Ctx->MonGroup.ReadFromHandoffBatchTimeout()++;
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

            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "SendPartsOnMain"),
                {"Marker", "BSVB29"},
                {"Parts", Reader.GetResult().size()});

            if (Reader.GetResult().empty()) {
                YDB_LOG_DEBUG(VDISKP(Ctx->VCtx, "Nothing to send. PassAway"),
                    {"Marker", "BSVB18"});
                PassAway();
                return;
            }

            Sender.SendPartsOnMain(SelfId(), Reader.GetResult());

            Schedule(Ctx->Cfg.SendBatchTimeout, new NActors::TEvents::TEvWakeup(SEND_TIMEOUT_TAG)); // send timeout
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
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "SendOnMainBatchTimeout"),
                {"Marker", "BSVB19"});
            Ctx->MonGroup.SendOnMainBatchTimeout()++;
            PassAway();
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted());
            YDB_LOG_INFO(VDISKP(Ctx->VCtx, "TSender::PassAway"),
                {"Marker", "BSVB28"});
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateSend,
            hFunc(NActors::TEvents::TEvWakeup, TimeoutSend)
            hFunc(TEvBlobStorage::TEvVPutResult, HandlePutResult)
            hFunc(TEvBlobStorage::TEvVMultiPutResult, HandlePutResult)

            cFunc(NPDisk::TEvChunkReadResult::EventType, [](){})  // read results received after timeout

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
            TVector<TPartInfo>&& parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , GInfo(ctx->GInfo)
            , Reader(Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter, ctx->ReplMonGroup.ReplPDiskReadThrottledMicrosecondsPtr(), GInfo->GetTopology().GType, Ctx->MonGroup)
            , Sender(ctx, GInfo, queueActorMapPtr, Ctx->ReplMonGroup)
        {}

        void Bootstrap() {
            ReadPartsFromDisk();
        }
    };
}

IActor* CreateSenderActor(
    TActorId notifyId,
    TVector<TPartInfo>&& parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TSender(notifyId, std::move(parts), queueActorMapPtr, ctx);
}

} // NBalancing
} // NKikimr
