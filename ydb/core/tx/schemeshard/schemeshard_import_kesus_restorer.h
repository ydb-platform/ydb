#pragma once

#include "schemeshard_identificators.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateKesusResourcesRestorer(
    NActors::TActorId schemeShard,
    ui64 importId,
    ui32 itemIdx,
    TPathId dstPathId,
    TTxId createTxId,
    Ydb::RateLimiter::Resources resources,
    ui32 retries);

} // namespace NKikimr::NSchemeShard
