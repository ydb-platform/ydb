#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {
namespace {
    class TReader {
    private:
        const size_t BatchSize;
        TPDiskCtxPtr PDiskCtx;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        const TBlobStorageGroupType GType;

        TVector<TPart> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:
        enum EReaderState {
            WAITING_PDISK_RESPONSES,
            FINISHED,
        };

        TReader(size_t batchSize, TPDiskCtxPtr pDiskCtx, TQueue<TPartInfo> parts, TReplQuoter::TPtr replPDiskReadQuoter, TBlobStorageGroupType gType)
            : BatchSize(batchSize)
            , PDiskCtx(pDiskCtx)
            , Parts(std::move(parts))
            , Quoter(replPDiskReadQuoter)
            , GType(gType)
            , Result(Reserve(BatchSize))
            , Responses(0)
            , ExpectedResponses(0)
        {}

        EReaderState DoJobQuant(const TActorContext &ctx) {
            if (ExpectedResponses != 0) {
                return WAITING_PDISK_RESPONSES;
            }
            if (Parts.empty()) {
                return FINISHED;
            }
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto item = Parts.front();
                Parts.pop();
                Result[i] = TPart{
                    .Key=item.Key,
                    .Ingress=item.Ingress
                };
                if (std::holds_alternative<TRope>(item.PartData)) {
                    Result[i].PartData = std::get<TRope>(item.PartData);
                    ++Responses;
                } else {
                    TDiskPart diskPart = std::get<TDiskPart>(item.PartData);
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
                        std::make_unique<IEventHandle>(PDiskCtx->PDiskId, ctx.SelfID, ev.release()),
                        diskPart.Size
                    );
                }
                ++ExpectedResponses;
            }
            return WAITING_PDISK_RESPONSES;
        }

        std::optional<TVector<TPart>> TryGetResults() {
            if (ExpectedResponses != 0 && ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return Result;
            }
            return std::nullopt;
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
            auto diskBlob = TDiskBlob(&data, Result[i].Ingress.LocalParts(GType), GType, key);
            Result[i].PartData = diskBlob.GetPart(key.PartId() - 1, &Result[i].PartData);
        }
    };


    class TSender : public TActorBootstrapped<TSender> {
        TActorId NotifyId;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
        TReader Reader;

        struct TStats {
            ui32 PartsRead = 0;
            ui32 PartsSent = 0;
        };
        TStats Stats;

        void DoJobQuant(const TActorContext &ctx) {
            auto status = Reader.DoJobQuant(ctx);
            if (auto batch = Reader.TryGetResults()) {
                Stats.PartsRead += batch->size();
                SendParts(*batch, ctx);
            }
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ " << "Reader status "
                        << ": " << (ui32)status << " " << Stats.PartsRead << " " << Stats.PartsSent);
            if (status == TReader::EReaderState::FINISHED && Stats.PartsRead == Stats.PartsSent) {
                Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
                Send(SelfId(), new NActors::TEvents::TEvPoison);
            }
        }

        void SendParts(const TVector<TPart>& batch, const TActorContext &ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                        << "Sending parts " << batch.size());
            for (const auto& part: batch) {
                auto vDiskId = GetVDiskId(*Ctx->GInfo, part.Key);
                LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                            Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                            << "Sending " << part.Key.ToString() << " to " << Ctx->GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))
                            << "; Data size = " << part.PartData.size());
                auto& queue = (*QueueActorMapPtr)[TVDiskIdShort(vDiskId)];
                auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(
                    part.Key, part.PartData, vDiskId,
                    true, nullptr,
                    TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob
                );
                TReplQuoter::QuoteMessage(
                    Ctx->VCtx->ReplNodeRequestQuoter,
                    std::make_unique<IEventHandle>(queue, SelfId(), ev.release()),
                    part.PartData.size()
                );
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev, const TActorContext &ctx) {
            Stats.PartsSent += 1;
            LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BALANCING,
                        Ctx->GInfo->GetTopology().GetOrderNumber(Ctx->VCtx->ShortSelfVDisk) << "$ "
                        << "Put result: " << ev->Get()->ToString());
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader.Handle)
            HFunc(TEvBlobStorage::TEvVPutResult, Handle)
            CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        );

    public:
        TSender(
            TActorId notifyId,
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : NotifyId(notifyId)
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , Reader(32, Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter, Ctx->GInfo->GetTopology().GType)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}

IActor* CreateSenderActor(
    TActorId notifyId,
    TQueue<TPartInfo> parts,
    TQueueActorMapPtr queueActorMapPtr,
    std::shared_ptr<TBalancingCtx> ctx
) {
    return new TSender(notifyId, parts, queueActorMapPtr, ctx);
}

}
