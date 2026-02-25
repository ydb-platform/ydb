#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

namespace NKikimr {

class TChunkKeeperCtx {
public:
    const TIntrusivePtr<TVDiskLogContext> LogCtx;
    const TActorId SkeletonId;

    TChunkKeeperCtx(TIntrusivePtr<TVDiskLogContext> logCtx,
            const TActorId& skeletonId);
};

} // NKikimr
