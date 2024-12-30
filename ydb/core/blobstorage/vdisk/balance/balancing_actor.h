#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>

namespace NKikimr {

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> ctx);

namespace NBalancing {
    IActor* CreateSenderActor(
        TActorId notifyId,
        TVector<TPartInfo>&& parts,
        TQueueActorMapPtr queueActorMapPtr,
        std::shared_ptr<TBalancingCtx> ctx
    );
    IActor* CreateDeleterActor(
        TActorId notifyId,
        TVector<TLogoBlobID>&& parts,
        TQueueActorMapPtr queueActorMapPtr,
        std::shared_ptr<TBalancingCtx> ctx
    );
} // NBalancing
} // NKikimr
