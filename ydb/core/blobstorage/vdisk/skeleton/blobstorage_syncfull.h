#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    class TDb;
    IActor *CreateHullSyncFullActor(
            const TIntrusivePtr<TVDiskConfig> &config,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorId &parentId,
            const TVDiskID &sourceVDisk,
            const TActorId &recipient,
            THullDsSnap &&fullSnap,
            const TKeyLogoBlob &keyLogoBlob,
            const TKeyBlock &keyBlock,
            const TKeyBarrier &keyBarrier,
            NKikimrBlobStorage::ESyncFullStage stage,
            std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result);

} // NKikimr
