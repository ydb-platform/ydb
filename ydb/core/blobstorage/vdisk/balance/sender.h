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
                
            }
            Reader.DoJobQuant(ctx);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader.Handle)
        );

    public:
        TSender(
            TQueue<TPartInfo> parts,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
            , Reader(SelfId(), 32, Ctx->PDiskCtx, std::move(parts))
        {}
    };
}
