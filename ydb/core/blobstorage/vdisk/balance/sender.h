#pragma once

#include "defs.h"
#include "reader.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

    class TSender : public TActorBootstrapped<TSender> {
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
        TReader Reader;

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void DoJobQuant(const TActorContext &ctx) {
            if (auto batch = Reader.TryGetResults()) {
                SendParts(*batch);
            }
            Reader.DoJobQuant(ctx);
        }

        void SendParts(const TVector<TPart>& batch) {
            for (const auto& part: batch) {
                auto vDiskId = GetVDiskId(part.Key, part.PartIdx);
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

        TVDiskID GetVDiskId(const TLogoBlobID& key, ui8 partIdx) {
            TBlobStorageGroupInfo::TOrderNums orderNums;
            TBlobStorageGroupInfo::TVDiskIds vdisks;
            Ctx->GInfo->GetTopology().PickSubgroup(key.Hash(), orderNums);
            for (const auto &x : orderNums) {
                vdisks.push_back(Ctx->GInfo->GetVDiskId(x));
            }
            return vdisks[partIdx]; // TODO: -1 ??
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr) {

        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader.Handle)
            hFunc(TEvBlobStorage::TEvVPutResult, Handle)
        );

    public:
        TSender(
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , Reader(SelfId(), 32, Ctx->PDiskCtx, std::move(parts), ctx->VCtx->ReplPDiskReadQuoter)
        {}
    };
}
