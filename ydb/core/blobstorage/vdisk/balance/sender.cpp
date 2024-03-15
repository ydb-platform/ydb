#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {
namespace NBalancing {
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

        EReaderState DoJobQuant(const TActorId& selfId) {
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
                std::visit(TOverloaded{
                    [&](const TRope& data) {
                        Result[i].PartData = data;
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
                    }
                }, item.PartData);
                ++ExpectedResponses;
            }
            return Responses != ExpectedResponses ? WAITING_PDISK_RESPONSES : FINISHED;
        }

        std::optional<TVector<TPart>> TryGetResults() {
            if (ExpectedResponses != 0 && ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return std::move(Result);
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
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TReader Reader;

        struct TStats {
            ui32 PartsRead = 0;
            ui32 PartsSent = 0;
            ui32 PartsSentUnsuccsesfull = 0;
        };
        TStats Stats;

        void DoJobQuant() {
            auto status = Reader.DoJobQuant(SelfId());
            if (auto batch = Reader.TryGetResults()) {
                Stats.PartsRead += batch->size();
                SendParts(*batch);
            }
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Reader status "
                    << ": " << (ui32)status << " " << Stats.PartsRead << " " << Stats.PartsSent);
            if (status == TReader::EReaderState::FINISHED && Stats.PartsRead == Stats.PartsSent) {
                PassAway();
            }
        }

        void SendParts(const TVector<TPart>& batch) {
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Sending parts " << batch.size());
            for (const auto& part: batch) {
                auto vDiskId = GetMainReplicaVDiskId(*GInfo, part.Key);
                BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Sending " << part.Key.ToString()
                        << " to " << GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))
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

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            ++Stats.PartsSent;
            if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
                ++Stats.PartsSentUnsuccsesfull;
                BLOG_W(Ctx->VCtx->VDiskLogPrefix << "Put failed: " << ev->Get()->ToString());
                return;
            }
            BLOG_D(Ctx->VCtx->VDiskLogPrefix << "Put result: " << ev->Get()->ToString());
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader.Handle)
            hFunc(TEvBlobStorage::TEvVPutResult, Handle)
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)

            hFunc(TEvVGenerationChange, Handle)
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
            , GInfo(ctx->GInfo)
            , Reader(32, Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter, GInfo->GetTopology().GType)
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

} // NBalancing
} // NKikimr
