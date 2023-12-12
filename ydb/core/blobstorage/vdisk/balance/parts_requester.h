#pragma once

#include "defs.h"
#include "utils.h"

#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {
    class TPartsRequester {
    private:
        const TActorId NotifyId;
        const size_t BatchSize;
        TQueue<TPartInfo> Parts;
        TReplQuoter::TPtr Quoter;
        TIntrusivePtr<TBlobStorageGroupInfo> gInfo;
        TQueueActorMapPtr QueueActorMapPtr;

        TVector<TPartOnMain> Result;
        ui32 Responses;
        ui32 ExpectedResponses;
    public:
        enum EState {
            WAITING_RESPONSES,
            FINISHED,
        };

        TPartsRequester(TActorId notifyId, size_t batchSize, TQueue<TPartInfo> parts, TReplQuoter::TPtr quoter, TQueueActorMapPtr queueActorMapPtr)
            : NotifyId(notifyId)
            , BatchSize(batchSize)
            , Parts(std::move(parts))
            , Quoter(quoter)
            , QueueActorMapPtr(queueActorMapPtr)
            , Result(Reserve(BatchSize))
            , Responses(0)
        {}

        EState DoJobQuant(const TActorContext &ctx) {
            if (ExpectedResponses != 0) {
                return WAITING_RESPONSES;
            }
            if (Parts.empty()) {
                return FINISHED;
            }
            Result.resize(Min(Parts.size(), BatchSize));
            ExpectedResponses = 0;
            for (ui64 i = 0; i < BatchSize && !Parts.empty(); ++i) {
                auto item = Parts.front();
                Parts.pop();
                Result[i] = TPartOnMain{
                    .Key=item.Key,
                    .Ingress=item.Ingress,
                    .HasOnMain=false
                };

                auto vDiskId = GetVDiskId(*gInfo, item.Key);
                auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                    vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, i,
                    {{item.Key.FullID(), 0, 0}}
                );
                TReplQuoter::QuoteMessage(
                    Quoter,
                    std::make_unique<IEventHandle>(QueueActorMapPtr->at(TVDiskIdShort(vDiskId)), ctx.SelfID, ev.release()),
                    0
                );
                ++ExpectedResponses;
            }
            return WAITING_RESPONSES;
        }

        std::optional<TVector<TPartOnMain>> TryGetResults() {
            if (ExpectedResponses != 0 && ExpectedResponses == Responses) {
                ExpectedResponses = 0;
                Responses = 0;
                return Result;
            }
            return std::nullopt;
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            ++Responses;
            auto msg = ev->Get()->Record;
            if (msg.GetStatus() != NKikimrProto::EReplyStatus::OK) {
                return;
            }
            ui64 i = msg.GetCookie();
            auto res = msg.GetResult().at(0);
            for (ui32 partIdx: res.GetParts()) {
                if (partIdx == Result[i].Key.PartId()) {
                    Result[i].HasOnMain = true;
                }
            }
        }
    };

}
