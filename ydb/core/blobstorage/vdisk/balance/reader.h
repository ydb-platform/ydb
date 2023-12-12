#pragma once

#include "defs.h"


namespace NKikimr {
    class TReader {
    private:
        const size_t BatchSize;
        TPDiskCtxPtr PDiskCtx;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;

        TVector<TPart> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:
        enum EReaderState {
            WAITING_PDISK_RESPONSES,
            FINISHED,
        };

        TReader(size_t batchSize, TPDiskCtxPtr pDiskCtx, TQueue<TPartInfo> parts, TReplQuoter::TPtr replPDiskReadQuoter)
            : BatchSize(batchSize)
            , PDiskCtx(pDiskCtx)
            , Parts(std::move(parts))
            , Quoter(replPDiskReadQuoter)
            , Result(Reserve(BatchSize))
            , Responses(0)
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
                } else {
                    TDiskPart diskPart = std::get<TDiskPart>(item.PartData);
                    auto ev = std::make_unique<NPDisk::TEvChunkRead>(
                        PDiskCtx->Dsk->Owner,
                        PDiskCtx->Dsk->OwnerRound,
                        diskPart.ChunkIdx,
                        diskPart.Offset,
                        diskPart.Size,
                        NPriRead::HullLow,
                        reinterpret_cast<void *>(i)

                    );
                    TReplQuoter::QuoteMessage(
                        Quoter,
                        std::make_unique<IEventHandle>(PDiskCtx->PDiskId, ctx.SelfID, ev.release()),
                        diskPart.Size
                    );
                    ++ExpectedResponses;
                }
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
            Result[i].PartData = TRope(msg->Data.ToString());
        }
    };

}
