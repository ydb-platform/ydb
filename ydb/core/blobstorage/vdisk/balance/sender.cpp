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
        const size_t BatchSize;
        TPDiskCtxPtr PDiskCtx;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        const TBlobStorageGroupType GType;

        TVector<TPart> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:

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

        void ScheduleJobQuant(const TActorId& selfId) {
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto item = Parts.front();
                Parts.pop();
                Result[i] = TPart{
                    .Key=item.Key,
                    .PartsMask=item.PartsMask,
                };
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
                    }
                }, item.PartData);
                ++ExpectedResponses;
            }
        }

        std::pair<std::optional<TVector<TPart>>, ui32> TryGetResults() {
            if (ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return {std::move(Result), Parts.size()};
            }
            return {std::nullopt, Parts.size()};
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

            for (ui8 partIdx = localParts.FirstPosition(); partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx)) {
                TRope result;
                result = diskBlob.GetPart(partIdx, &result);
                Result[i].PartsData.emplace_back(std::move(result));
            }
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

        void ScheduleJobQuant() {
            Reader.ScheduleJobQuant(SelfId());
            // if all parts are already in memory, we could process results right away
            TryProcessResults();
        }

        void TryProcessResults() {
            if (auto [batch, partsLeft] = Reader.TryGetResults(); batch.has_value()) {
                Stats.PartsRead += batch->size();
                SendParts(*batch);

                // notify about job quant completion
                Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID, partsLeft));

                if (partsLeft == 0) {
                    // no more parts to send
                    PassAway();
                }
            }
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            Reader.Handle(ev);
            TryProcessResults();
        }

        void SendParts(const TVector<TPart>& batch) {
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB11, VDISKP(Ctx->VCtx, "Sending parts"), (BatchSize, batch.size()));

            for (const auto& part: batch) {
                auto localParts = part.PartsMask;
                for (ui8 partIdx = localParts.FirstPosition(), i = 0; partIdx < localParts.GetSize(); partIdx = localParts.NextPosition(partIdx), ++i) {
                    auto key = TLogoBlobID(part.Key, partIdx + 1);
                    const auto& data = part.PartsData[i];
                    auto vDiskId = GetMainReplicaVDiskId(*GInfo, key);
                    STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB12, VDISKP(Ctx->VCtx, "Sending"), (LogoBlobId, key.ToString()),
                        (To, GInfo->GetTopology().GetOrderNumber(TVDiskIdShort(vDiskId))), (DataSize, data.size()));

                    auto& queue = (*QueueActorMapPtr)[TVDiskIdShort(vDiskId)];
                    auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(
                        key, data, vDiskId,
                        true, nullptr,
                        TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::AsyncBlob
                    );
                    TReplQuoter::QuoteMessage(
                        Ctx->VCtx->ReplNodeRequestQuoter,
                        std::make_unique<IEventHandle>(queue, SelfId(), ev.release()),
                        data.size()
                    );
                }
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            ++Stats.PartsSent;
            if (ev->Get()->Record.GetStatus() != NKikimrProto::OK) {
                ++Stats.PartsSentUnsuccsesfull;
                STLOG(PRI_WARN, BS_VDISK_BALANCING, BSVB13, VDISKP(Ctx->VCtx, "Put failed"), (Msg, ev->Get()->ToString()));
                return;
            }
            STLOG(PRI_DEBUG, BS_VDISK_BALANCING, BSVB14, VDISKP(Ctx->VCtx, "Put done"), (Msg, ev->Get()->ToString()));
        }

        void PassAway() override {
            Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvVGenerationChange::TPtr ev) {
            GInfo = ev->Get()->NewInfo;
        }

        STRICT_STFUNC(StateFunc,
            cFunc(NActors::TEvents::TEvWakeup::EventType, ScheduleJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Handle)
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
