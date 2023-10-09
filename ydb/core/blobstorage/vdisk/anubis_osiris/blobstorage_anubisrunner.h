#pragma once

#include "defs.h"
#include "blobstorage_anubis.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisDone
    // Anubis job done
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisDone :
        public TEventLocal<TEvAnubisDone, TEvBlobStorage::EvAnubisDone>
    {
        TAnubisIssues Issues;

        TEvAnubisDone(const TAnubisIssues &issues)
            : Issues(issues)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvFullSyncedWith
    // Notify Anubis Runner that we full synced with this disk
    ////////////////////////////////////////////////////////////////////////////
    struct TEvFullSyncedWith :
        public TEventLocal<TEvFullSyncedWith, TEvBlobStorage::EvFullSyncedWith>
    {
        const TVDiskIdShort VDiskIdShort;

        TEvFullSyncedWith(const TVDiskIdShort &vd)
            : VDiskIdShort(vd)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // ANUBIS RUNNER ACTOR CREATOR
    // The actor tracks full syncs with other VDisks. When this VDisk gets out
    // of sync with BlobStorage group it can leave some LogoBlobs with Keep bits
    // forever, so additional activities are required for cleaning garbage.
    // These activities include
    // 1. Hull Db index scan and selecting LogoBlobIDs with Keep flags
    // 2. Check these LogoBlobIDs against other VDisks in the group to find
    //    those that must be garbage collected
    ////////////////////////////////////////////////////////////////////////////
    struct TAnubisCtx {
        TIntrusivePtr<THullCtx> HullCtx;
        TActorId SkeletonId;
        ui32 ReplInterconnectChannel;
        ui64 AnubisOsirisMaxInFly;
        TDuration AnubisTimeout;

        TAnubisCtx(const TIntrusivePtr<THullCtx> &hullCtx, const TActorId &skeletonId,
                ui32 replInterconnectChannel, ui64 anubisOsirisMaxInFly, TDuration anubisTimeout)
            : HullCtx(hullCtx)
            , SkeletonId(skeletonId)
            , ReplInterconnectChannel(replInterconnectChannel)
            , AnubisOsirisMaxInFly(anubisOsirisMaxInFly)
            , AnubisTimeout(anubisTimeout)
        {}
    };
    IActor* CreateAnubisRunner(const std::shared_ptr<TAnubisCtx> &anubisCtx,
                               const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo);

} // NKikimr
