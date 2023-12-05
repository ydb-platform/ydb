#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

    class TDeleter {
    private:
        TQueue<TPartInfo> Keys;
        TQueueActorMapPtr QueueActorMapPtr;
        std::shared_ptr<TBalancingCtx> Ctx;
    
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
