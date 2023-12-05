#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

    class TDeleter : public TActorBootstrapped<TDeleter> {
        TQueue<TPartInfo> Keys;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void DoJobQuant(const TActorContext &ctx) {
            Y_UNUSED(ctx);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
        );
    
    public:
        TDeleter() = default;
        TDeleter(
            TQueue<TPartInfo> keys,
            TQueueActorMapPtr queueActorMapPtr,
            std::shared_ptr<TBalancingCtx> ctx
        )
            : Keys(std::move(keys))
            , QueueActorMapPtr(queueActorMapPtr)
            , Ctx(ctx)
        {
        }

        void DoJobQuant() {

        }
    };
}
