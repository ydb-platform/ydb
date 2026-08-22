#include "chunk_keeper_ctx.h"

namespace NKikimr {

TChunkKeeperCtx::TChunkKeeperCtx(TIntrusivePtr<TVDiskLogContext> logCtx,
        const TActorId& skeletonId)
    : LogCtx(std::move(logCtx))
    , SkeletonId(skeletonId)
{}

} // namespace NKikimr
