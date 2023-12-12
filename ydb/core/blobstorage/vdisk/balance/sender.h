#pragma once

#include "defs.h"
#include "reader.h"
#include "utils.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

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
            if (auto batch = Reader.TryGetResults()) {
                Stats.PartsRead += batch->size();
                SendParts(*batch);
            }
            auto status = Reader.DoJobQuant(ctx);
            if (status == TReader::EReaderState::FINISHED && Stats.PartsRead == Stats.PartsSent) {
                Send(NotifyId, new NActors::TEvents::TEvCompleted(SENDER_ID));
                Die(ctx);
            }
        }

        void SendParts(const TVector<TPart>& batch) {
            for (const auto& part: batch) {
                auto vDiskId = GetVDiskId(*Ctx->GInfo, part.Key);
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

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr) {
            Stats.PartsSent += 1;
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader.Handle)
            hFunc(TEvBlobStorage::TEvVPutResult, Handle)
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
            , Reader(32, Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }
    };
}
