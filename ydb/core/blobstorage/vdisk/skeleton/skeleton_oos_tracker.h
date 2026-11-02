#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    struct TEvDiskSpaceUpdated : TEventLocal<TEvDiskSpaceUpdated, TEvBlobStorage::EvDiskSpaceUpdated> {};

    ////////////////////////////////////////////////////////////////////////////
    // CreateDskSpaceTracker -- tracks (refreshes) actual state of 'out of disk space'
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    class TPDiskCtx;
    IActor* CreateDskSpaceTracker(
                TIntrusivePtr<TVDiskContext> vctx,
                std::shared_ptr<TPDiskCtx> pdiskCtx,
                TDuration dskTrackerInterval,
                TActorId notifyId);

} // NKikimr
