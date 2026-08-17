#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NNbsDbgLike {

// Hive-managed persistent load tablet. The tablet:
//   - allocates and persists N DBGs via TEvControllerAllocateDDiskBlockGroup
//   - opens 10*N DDisk/PB connections at boot
//   - releases all DBGs on TEvDelete (TargetNumVChunks=0) and clears the schema
NActors::IActor* CreateNbsDbgLikeLoadTablet(
    const NActors::TActorId& tablet,
    TTabletStorageInfo* info);

} // namespace NKikimr::NNbsDbgLike
