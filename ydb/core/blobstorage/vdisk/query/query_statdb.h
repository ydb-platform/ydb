#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

namespace NKikimr {

    IActor* CreateLevelIndexStatActor(
        const TIntrusivePtr<THullCtx>& hullCtx,
        const TActorId& parentId,
        TLogoBlobsSnapshot&& snapshot,
        TEvBlobStorage::TEvVDbStat::TPtr& ev,
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);

    IActor* CreateLevelIndexStatActor(
        const TIntrusivePtr<THullCtx>& hullCtx,
        const TActorId& parentId,
        TBlocksSnapshot&& snapshot,
        TEvBlobStorage::TEvVDbStat::TPtr& ev,
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);

    IActor* CreateLevelIndexStatActor(
        const TIntrusivePtr<THullCtx>& hullCtx,
        const TActorId& parentId,
        TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>&& snapshot,
        TEvBlobStorage::TEvVDbStat::TPtr& ev,
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);

    IActor* CreateLevelIndexStatActor(
        const TIntrusivePtr<THullCtx>& hullCtx,
        const TActorId& parentId,
        TLogoBlobsSnapshot&& snapshot,
        TEvGetLogoBlobIndexStatRequest::TPtr& ev,
        std::unique_ptr<TEvGetLogoBlobIndexStatResponse> result);

} // NKikimr
